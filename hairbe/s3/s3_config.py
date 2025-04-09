import os
from pydantic import BaseModel

class S3Config(BaseModel):
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    REGION_NAME: str = os.getenv("REGION_NAME")
    BUCKET_NAME: str = os.getenv("BUCKET_NAME")

    READ_PRESIGNED_URL_EXPIRATION_SEC: int = 3600
    WRITE_PRESIGNED_URL_EXPIRATION_SEC: int = 60

    USER_FACE_IMAGE_PATH_PREFIX: str = "user_face_image"

s3_config = S3Config()