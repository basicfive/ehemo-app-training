from pydantic import BaseModel
import time
import os
import json
import subprocess
import asyncio
import logging
import multiprocessing
import queue
from typing import Tuple, List, Optional
from datetime import datetime
from enum import Enum, auto
from ehemo.config import training_config
from ehemo.mq.mq_config import rabbit_mq_config
from ehemo.mq.mq_service import AsyncRabbitMQService
from ehemo.s3.s3_service import S3Client
from ehemo.dto.training import TrainingJobMQConsumeMessage, TrainingJobMQPublishMessage, Gender
import toml
from ehemo.ehemo_utils import get_extension_from_content_type

logger = logging.getLogger(__name__)

class RequestHandler:
    """MQ 메시지 처리 및 결과 발행만 담당하는 핸들러"""
    
    def __init__(
        self,
        message_publisher: AsyncRabbitMQService,
        request_queue: multiprocessing.Queue,
        result_queue: multiprocessing.Queue
    ):
        self.message_publisher = message_publisher
        self.request_queue = request_queue
        self.result_queue = result_queue

    async def handle_training_request(self, body: bytes):
        """MQ 메시지 처리 - 학습 완료까지 동기 대기"""
        dict_data = json.loads(body)
        message = TrainingJobMQConsumeMessage(**dict_data)
            
        logger.info(f"MQ 핸들러 - 학습 요청 수신: {message.training_job_id}")
        
        # 워커에게 요청 전달
        self.request_queue.put_nowait(message.model_dump())
        logger.info(f"MQ 핸들러 - 워커에게 요청 전달: {message.training_job_id}")
        
        # 학습 완료까지 대기 (폴링 방식)
        logger.info(f"MQ 핸들러 - 학습 완료 대기 시작: {message.training_job_id}")
        
        while True:
            try:
                # 결과 큐에서 결과 확인 (논블로킹)
                result_data = self.result_queue.get_nowait()
                
                # pydantic 모델로 역직렬화
                result = TrainingJobMQPublishMessage.model_validate(result_data)
                
                # 해당 작업의 결과인지 확인
                if result.training_job_id == message.training_job_id:
                    logger.info(f"MQ 핸들러 - 학습 완료: {message.training_job_id}, 성공: {result.is_success}")
                    
                    # MQ로 결과 발행
                    await self.message_publisher.publish_message(
                        publish_queue=rabbit_mq_config.RABBITMQ_TRAINING_PUBLISH,
                        message=result.model_dump_json(),
                    )
                    logger.info(f"MQ 핸들러 - 결과 발행 완료: {message.training_job_id}")
                    
                    # 함수 종료 → MQ ACK → 다음 메시지 수신 가능
                    return
                else:
                    # 다른 작업의 결과라면 다시 큐에 넣기
                    self.result_queue.put(result_data)
                    
            except queue.Empty:
                # 결과가 없으면 잠시 대기 후 다시 확인
                await asyncio.sleep(1)
                continue


