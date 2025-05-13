import ssl
import logging
import asyncio
import aio_pika
from urllib.parse import quote
from typing import Callable

from ehemo.mq.mq_config import rabbit_mq_config

logger = logging.getLogger(__name__)

class AsyncRabbitMQService:
    def __init__(
        self,
        host: str = rabbit_mq_config.RABBITMQ_HOST,
        port: int = rabbit_mq_config.RABBITMQ_PORT,
        username: str = rabbit_mq_config.RABBITMQ_USERNAME,
        password: str = rabbit_mq_config.RABBITMQ_PASSWORD,
        vhost: str = rabbit_mq_config.RABBITMQ_VHOST,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        
        self.connection = None
        self.channel = None
    
    async def connect(self, initial_retries=5, initial_retry_delay=5, extended_retry_delay=60):
        """RabbitMQ에 비동기적으로 연결합니다."""
        retry_count = 0
        while True:
            try:
                await self._connect()
                return
            except Exception as e:  # 모든 예외를 일단 잡아서 로깅하고 재시도 결정
                retry_count += 1
                delay = initial_retry_delay if retry_count <= initial_retries else extended_retry_delay
                log_message = (
                    f"연결 실패 ({type(e).__name__}: {e}), {delay}초 후 재시도 ({retry_count}/{initial_retries})..."
                    if retry_count <= initial_retries
                    else f"초기 재시도 횟수 초과 ({type(e).__name__}: {e}), {delay}초 후 재시도 중... (시도 횟수: {retry_count})"
                )
                logger.error(log_message)
                await asyncio.sleep(delay)

    async def _connect(self):
        """내부 연결 로직"""
        if self.connection and not self.connection.is_closed:
            await self.close()  # 이전 연결/채널 정리
            
        # SSL 컨텍스트 설정
        ssl_context = ssl.create_default_context()

        # 비밀번호와 vhost URL 인코딩
        encoded_password = quote(self.password, safe='')
        encoded_vhost = quote(self.vhost, safe='') if self.vhost else ''
        
        # 연결 문자열 구성 (SSL 사용시 amqps://)
        connection_string = f"amqps://{self.username}:{encoded_password}@{self.host}:{self.port}/{encoded_vhost}"
        logger.info(f"RabbitMQ 연결 시도 중... Host: {self.host}, Port: {self.port}, VHost: {self.vhost}")
        
        # 연결 및 채널 생성
        self.connection = await aio_pika.connect_robust(
            connection_string,
            ssl=True,
            ssl_context=ssl_context,
            heartbeat=360,  # 하트비트 간격 설정 (기존 설정과 동일하게 360초)
        )
        
        self.channel = await self.connection.channel()
        logger.info("RabbitMQ 연결 성공!")

    async def ensure_queue_exists(self, queue_name: str):
        """큐가 존재하는지 확인합니다. 없으면 에러를 반환합니다."""
        try:
            # 채널이 없거나 닫혀 있으면 재연결
            if not self.channel or self.channel.is_closed:
                await self._connect()
                
            # 큐 선언 (passive=True는 큐가 존재하는지만 확인함)
            await self.channel.declare_queue(queue_name, passive=True)
            logger.info(f"큐 '{queue_name}'가 존재합니다.")
            return True
        except aio_pika.exceptions.ChannelNotFoundEntity:
            logger.error(f"큐 '{queue_name}'가 존재하지 않습니다.")
            return False
        except Exception as e:
            logger.error(f"큐 상태 확인 중 오류 발생: {e}")
            return False

    async def publish_message(self, publish_queue: str, message: str, durable_message: bool = True, max_retries=3, retry_delay=5):
        """비동기적으로 메시지를 발행합니다."""
        # 큐가 있는지 확인
        if not await self.ensure_queue_exists(publish_queue):
            raise Exception(f"큐 '{publish_queue}'가 존재하지 않습니다. 메시지를 발행할 수 없습니다.")
            
        retry_count = 0
        while retry_count < max_retries:
            try:
                # 채널이 없거나 닫혀 있으면 재연결
                if not self.channel or self.channel.is_closed:
                    await self._connect()
                    # 재연결 후 큐 존재 여부 다시 확인
                    if not await self.ensure_queue_exists(publish_queue):
                        raise Exception(f"재연결 후 큐 '{publish_queue}'가 존재하지 않습니다.")
                
                # 메시지 발행
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message.encode('utf-8'),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT if durable_message else aio_pika.DeliveryMode.NOT_PERSISTENT
                    ),
                    routing_key=publish_queue
                )
                
                logger.info(f"메시지가 큐 '{publish_queue}'에 성공적으로 발행되었습니다.")
                return True
                
            except aio_pika.exceptions.ChannelNotFoundEntity as e:  # 큐가 존재하지 않음
                logger.error(f"메시지 발행 실패: 큐 '{publish_queue}'를 찾을 수 없습니다 ({e}). 이 오류는 상위로 전파됩니다.")
                raise
                
            except Exception as e:  # 연결 문제 또는 기타 오류
                retry_count += 1
                logger.error(f"메시지 발행 중 오류 발생 ({type(e).__name__}: {e}), 재시도 {retry_count}/{max_retries}...")
                if retry_count >= max_retries:
                    logger.error("최대 재시도 횟수 초과")
                    raise
                await asyncio.sleep(retry_delay)
                await self._connect()
        return False

    async def start_consuming(self, consume_queue: str, process_message_func: Callable):
        """비동기적으로 메시지 소비를 시작합니다."""
        while True:
            try:
                # 채널이 없거나 닫혀 있으면 재연결
                if not self.channel or self.channel.is_closed:
                    await self._connect()
                
                # 큐 선언 (passive=True는 큐가 존재하는지만 확인함)
                queue = await self.channel.declare_queue(consume_queue, passive=True)
                
                logger.info(f"큐 '{consume_queue}'에서 메시지 소비를 시작합니다...")
                
                # 비동기 처리용 내부 콜백
                async def _on_message(message: aio_pika.IncomingMessage):
                    try:
                        logger.info(f"수신된 메시지: {message.body}")
                        
                        # 메시지 처리 (비동기적으로 호출하고, 완료될 때까지 대기)
                        await process_message_func(message.body)
                        
                        # 모든 처리가 완료된 후 ack 전송
                        await message.ack()
                        logger.info(f"메시지 처리 완료 및 ack 전송: {message.message_id}")
                    except Exception as e:
                        logger.error(f"메시지 처리 중 오류 발생: {e}")
                        # 오류 발생 시에도 ack 처리 (재처리 방지, 필요시 nack로 변경 가능)
                        await message.ack()
                
                # prefetch_count=1로 설정하여 한 번에 하나의 메시지만 가져오도록 함
                await self.channel.set_qos(prefetch_count=1)
                
                # 메시지 소비 시작 (no_ack=False로 설정하여 명시적 ack 사용)
                consumer_tag = await queue.consume(_on_message, no_ack=False)
                logger.info(f"메시지 소비 시작 (Consumer tag: {consumer_tag})")
                
                # 이벤트 루프가 종료되지 않도록 유지
                try:
                    # 무한 대기
                    await asyncio.Future()
                except asyncio.CancelledError:
                    # 취소 요청 (예: Ctrl+C)
                    logger.info("소비 작업이 취소되었습니다.")
                    break
                    
            except aio_pika.exceptions.ChannelNotFoundEntity as e:  # 큐가 존재하지 않음
                logger.error(f"메시지 소비 실패: 큐 '{consume_queue}'를 찾을 수 없습니다 ({e}). 소비를 중단하고 이 오류는 상위로 전파됩니다.")
                await self.close()
                raise
                
            except asyncio.CancelledError:
                # 취소 요청 (예: Ctrl+C)
                logger.info("소비 작업이 취소되었습니다.")
                await self.close()
                break
                
            except Exception as e:  # 연결 문제 또는 기타 오류
                logger.error(f"메시지 소비 중 오류 발생 ({type(e).__name__}: {e}), 재연결 후 소비를 재개합니다...")
                # 짧은 대기 후 재연결 시도
                await asyncio.sleep(5)
                await self._connect()

    async def close(self):
        """연결과 채널을 비동기적으로 닫습니다."""
        try:
            if self.channel and not self.channel.is_closed:
                logger.info("채널을 닫습니다...")
                await self.channel.close()
                
            if self.connection and not self.connection.is_closed:
                logger.info("연결을 닫습니다...")
                await self.connection.close()
                
            logger.info("RabbitMQ 연결 및 채널 종료 완료")
        except Exception as e:
            logger.error(f"연결 또는 채널 종료 중 오류 발생: {e}")
        finally:
            self.channel = None
            self.connection = None 