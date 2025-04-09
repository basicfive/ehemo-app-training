from hairbe.mq.mq_service import RabbitMQService
from hairbe.s3.s3_service import S3Client
from hairbe.handle_request import RequestHandler
from hairbe.mq.mq_config import rabbit_mq_config

mq_service = RabbitMQService()
s3_client = S3Client()

request_handler = RequestHandler(s3_client=s3_client, message_publisher=mq_service)

mq_service.start_consuming(
    consume_queue=rabbit_mq_config.RABBITMQ_TRAINING_CONSUME,
    handle_message=request_handler.handle_training_request,
)