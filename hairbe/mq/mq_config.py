import os
from pydantic import BaseModel

class RabbitMQConfig(BaseModel):
    RABBITMQ_HOST: str = os.getenv('RABBITMQ_HOST')
    RABBITMQ_PORT: int = os.getenv('RABBITMQ_PORT')
    RABBITMQ_USERNAME: str = os.getenv('RABBITMQ_USERNAME')
    RABBITMQ_PASSWORD: str = os.getenv('RABBITMQ_PASSWORD')
    RABBITMQ_VHOST: str = os.getenv('RABBITMQ_VHOST')

    RABBITMQ_TRAINING_CONSUME: str = os.getenv('RABBITMQ_TRAINING_CONSUME')
    RABBITMQ_TRAINING_PUBLISH: str = os.getenv('RABBITMQ_TRAINING_PUBLISH')

rabbit_mq_config = RabbitMQConfig()