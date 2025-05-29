import os
import asyncio
import logging
import multiprocessing
from dotenv import load_dotenv

from ehemo.mq.mq_service import AsyncRabbitMQService
from ehemo.request_handler import RequestHandler
from ehemo.training_worker import start_training_worker
from ehemo.mq.mq_config import rabbit_mq_config
from ehemo.s3.s3_service import get_s3_client

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

# 환경 변수 로드
load_dotenv()


async def main():
    """메인 애플리케이션 실행"""
    mq_service = None
    request_handler = None
    worker_process = None
    
    try:
        # S3 클라이언트 생성
        s3_client = get_s3_client()
        logger.info("S3 클라이언트 초기화 완료")
        
        # 멀티프로세싱 큐 생성
        request_queue = multiprocessing.Queue(maxsize=10)  # 요청 대기열 크기 제한
        result_queue = multiprocessing.Queue()  # 결과 큐
        
        # 단일 워커 프로세스 시작
        worker_process = multiprocessing.Process(
            target=start_training_worker,
            args=(request_queue, result_queue, s3_client),
            name="TrainingWorker"
        )
        worker_process.start()
        logger.info(f"학습 워커 프로세스 시작: {worker_process.name} (PID: {worker_process.pid})")
        
        # RabbitMQ 서비스 초기화
        mq_service = AsyncRabbitMQService()
        await mq_service.connect()
        
        # 요청 핸들러 초기화 (큐들을 전달)
        request_handler = RequestHandler(mq_service, request_queue, result_queue)
        
        logger.info(f"애플리케이션 시작 - MQ 소비 큐: {rabbit_mq_config.RABBITMQ_TRAINING_CONSUME}")
        
        # MQ 메시지 소비 시작
        await mq_service.start_consuming(
            consume_queue=rabbit_mq_config.RABBITMQ_TRAINING_CONSUME,
            process_message_func=request_handler.handle_training_request
        )
        
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 오류 발생: {e}")
    finally:
        # 리소스 정리
        if mq_service:
            await mq_service.close()
        if worker_process:
            logger.info("워커 프로세스 종료 시작...")
            
            # 워커에게 종료 신호 전송
            try:
                request_queue.put_nowait(None)  # 종료 신호
            except Exception:
                pass
            
            # 워커 프로세스 종료 대기
            try:
                worker_process.join(timeout=10)  # 10초 대기
                if worker_process.is_alive():
                    logger.warning(f"워커 프로세스 강제 종료: {worker_process.name}")
                    worker_process.terminate()
                    worker_process.join(timeout=5)
                    if worker_process.is_alive():
                        worker_process.kill()
                logger.info(f"워커 프로세스 종료 완료: {worker_process.name}")
            except Exception as e:
                logger.error(f"워커 프로세스 종료 중 오류: {worker_process.name}, {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("애플리케이션이 사용자에 의해 종료되었습니다.")
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 예상치 못한 오류 발생: {e}") 