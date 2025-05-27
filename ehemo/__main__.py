import os
import asyncio
import logging
from dotenv import load_dotenv

from ehemo.s3.s3_service import S3Client
from ehemo.mq.mq_service import AsyncRabbitMQService
from ehemo.handle_request import RequestHandler
from ehemo.mq.mq_config import rabbit_mq_config

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
    try:
        # S3 클라이언트 초기화
        s3_client = S3Client()
        
        # RabbitMQ 서비스 초기화 및 연결
        mq_service = AsyncRabbitMQService()
        
        # 메시지 핸들러 초기화 (RequestHandler 사용)
        request_handler = RequestHandler(s3_client, mq_service)

        await mq_service.connect()
        
        # 메시지 소비 시작
        logger.info(f"메시지 소비 시작 - 큐: {rabbit_mq_config.RABBITMQ_TRAINING_CONSUME}")
        await mq_service.start_consuming(
            consume_queue=rabbit_mq_config.RABBITMQ_TRAINING_CONSUME,
            process_message_func=request_handler.handle_training_request
        )
        
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 오류 발생: {e}")
        if 'mq_service' in locals():
            await mq_service.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("애플리케이션이 사용자에 의해 종료되었습니다.")
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 예상치 못한 오류 발생: {e}") 