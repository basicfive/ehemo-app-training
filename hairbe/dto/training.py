from pydantic import BaseModel
from enum import Enum
from typing import List

class Gender(str, Enum):
    MALE = "MALE"
    FEMALE = "FEMALE"

class UserFaceTrainingJobMQConsumeMessage(BaseModel):
    gender: Gender
    user_face_training_job_id: str
    model_s3_key: str
    s3_key_list: List[str]
    lora_model_name: str

class UserFaceTrainingJobMQPublishMessage(BaseModel):
    is_success: bool
    user_face_training_job_id: str
    model_s3_key: str
    lora_model_name: str
    actual_training_time_sec: int
