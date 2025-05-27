"""
EHEMO Training Application
비동기 MQ 통신 방식 적용 (aio_pika)
"""

__version__ = "2.0.0"

from dotenv import load_dotenv
import os

env = os.getenv('ENVIRONMENT', 'dev')
load_dotenv(f'.env.{env}')

from ehemo.config import *