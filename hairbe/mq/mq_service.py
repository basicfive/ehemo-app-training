import subprocess
import pika
import json
import os
import threading
import time
import sys
import ssl
from pydantic import BaseModel
from hairbe.mq.mq_config import rabbit_mq_config
from typing import Callable

class RabbitMQService:
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
        
        # 연결과 채널 속성 초기화
        self.connection = None
        self.channel = None
       
        # MQ 연결
        self.connect()

    def connect(self, max_retries=5, retry_delay=5):
        """연결을 설정하고 채널을 생성합니다. 실패 시 재시도합니다."""
        retry_count = 0
        while retry_count < max_retries:
            try:
                self._connect()
                return
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"최대 재시도 횟수 초과: {e}")
                    raise
                print(f"연결 실패, {retry_delay}초 후 재시도 ({retry_count}/{max_retries})...")
                time.sleep(retry_delay)

    def _connect(self):
        """실제 연결을 설정하는 내부 메서드"""
        if hasattr(self, 'connection') and self.connection and self.connection.is_open:
            self.close()
            
        cred = pika.PlainCredentials(username=self.username, password=self.password)

        # SSL 컨텍스트 설정
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        # 인증서 검증을 비활성화 (개발 환경에서만 사용하세요)
        ssl_context.verify_mode = ssl.CERT_NONE
        
        ssl_options = pika.SSLOptions(context=ssl_context, server_hostname=self.host)

        # 연결 파라미터 설정
        conn_params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=cred,
            ssl_options=ssl_options,
            heartbeat=360
        )

        print(f"RabbitMQ 연결 시도 중... Host: {self.host}, Port: {self.port}, VHost: {self.vhost}")
        self.connection = pika.BlockingConnection(conn_params)
        self.channel = self.connection.channel()
        print("RabbitMQ 연결 성공!")

    def ensure_queue_exists(self, queue_name: str):
        """큐가 존재하는지 확인합니다. 없으면 에러를 반환합니다."""
        try:
            # queue_declare의 passive=True 옵션은 큐가 있는지만 확인하고 없으면 예외를 발생시킵니다
            self.channel.queue_declare(queue=queue_name, passive=True)
            print(f"큐 '{queue_name}'가 존재합니다.")
            return True
        except pika.exceptions.ChannelClosedByBroker:
            # 채널이 닫히면 다시 연결합니다
            print(f"큐 '{queue_name}'가 존재하지 않습니다.")
            self.reconnect(0)  # 즉시 재연결 (지연 없음)
            return False
        except Exception as e:
            print(f"큐 상태 확인 중 오류 발생: {e}")
            return False
        
    def publish_message(self, publish_queue: str, message: str, max_retries=3, retry_delay=5):
        """메시지를 발행하고, 연결 문제 발생 시 재연결을 시도합니다."""
        # 큐가 있는지 확인
        if not self.ensure_queue_exists(publish_queue):
            raise Exception(f"큐 '{publish_queue}'가 존재하지 않습니다. 메시지를 발행할 수 없습니다.")
            
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=publish_queue,
                    body=message
                )
                return True
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as e:
                retry_count += 1
                print(f"발행 중 오류 발생: {e}, 재시도 {retry_count}/{max_retries}...")
                if not self.reconnect(retry_delay):
                    continue
                # 재연결 후 큐 존재 여부 다시 확인
                if not self.ensure_queue_exists(publish_queue):
                    raise Exception(f"재연결 후 큐 '{publish_queue}'가 존재하지 않습니다.")
                if retry_count >= max_retries:
                    print("최대 재시도 횟수 초과")
                    raise
        return False
    

    def start_consuming(self, consume_queue: str, handle_message: Callable):
        """메시지 소비를 시작하고, 연결 문제 발생 시 재연결을 시도합니다."""
        reconnect_delay = 5  # 초기 재연결 대기 시간
        max_reconnect_delay = 60  # 최대 재연결 대기 시간
        
        while True:
            # 큐가 있는지 확인
            if not self.ensure_queue_exists(consume_queue):
                raise Exception(f"큐 '{consume_queue}'가 존재하지 않습니다. 메시지를 소비할 수 없습니다.")
                
            try:
                # 채널 설정
                self.channel.basic_qos(prefetch_count=1)
                self.channel.basic_consume(
                    queue=consume_queue,
                    on_message_callback=handle_message,
                    auto_ack=True
                )
                print(f"starting to consume messages on {consume_queue}...")
                
                # 메시지 소비 시작
                self.channel.start_consuming()
                
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as e:
                print(f"소비 중 오류 발생: {e}, {reconnect_delay}초 후 재연결 시도...")
                
                # 연결 재시도
                if self.reconnect(reconnect_delay):
                    # 성공하면 재연결 지연 시간 초기화
                    reconnect_delay = 5
                else:
                    # 실패하면 지연 시간을 증가시키되 최대값 제한
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                
            except Exception as e:
                print(f"예상치 못한 오류 발생: {e}")
                self.close()
                raise

    def reconnect(self, delay=5):
        """연결이 끊어진 경우 재연결을 시도합니다."""
        print(f"재연결 시도 중... {delay}초 대기")
        time.sleep(delay)
        try:
            self.connect()
            print("재연결 성공!")
            return True
        except Exception as e:
            print(f"재연결 실패: {e}")
            return False

    def close(self):
        """연결과 채널을 닫습니다."""
        try:
            if hasattr(self, 'channel') and self.channel and self.channel.is_open:
                self.channel.close()
            if hasattr(self, 'connection') and self.connection and self.connection.is_open:
                self.connection.close()
            print("연결 종료 완료")
        except Exception as e:
            print(f"연결 종료 중 오류: {e}")
