from pydantic import BaseModel
from enum import Enum
from typing import List

class Gender(str, Enum):
    MALE = "MALE"
    FEMALE = "FEMALE"

class TrainingJobMQConsumeMessage(BaseModel):
    gender: Gender
    training_job_id: int
    user_hair_lora_s3_key: str
    user_hair_lora_name: str
    uploaded_image_s3_keys: List[str]
    total_steps: int
    epoch: int

class TrainingJobMQPublishMessage(BaseModel):
    training_job_id: int
    is_success: bool
    user_hair_lora_s3_key: str
    user_hair_lora_name: str
    actual_training_time_sec: int