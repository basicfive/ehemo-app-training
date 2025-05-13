import logging
from typing import Optional, Dict, Any, Tuple, Union, BinaryIO
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
import os

from ehemo.s3.s3_config import s3_config

class S3Client:
    def __init__(
            self,
            aws_access_key_id: str = s3_config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key: str = s3_config.AWS_SECRET_ACCESS_KEY,
            region_name: str = s3_config.REGION_NAME,
            bucket_name: str = s3_config.BUCKET_NAME
    ):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.bucket_name = bucket_name

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
            config=Config(signature_version='s3v4')
        )

    def download_from_s3(self, s3_key: str) -> Tuple[Optional[bytes], Optional[str]]:
        """
        S3에서 객체를 직접 다운로드

        Args:
            s3_key (str): 다운로드할 S3 객체 키

        Returns:
            Tuple[Optional[bytes], Optional[str]]: (객체 데이터, ContentType) 또는 (None, None) 실패 시
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            # 객체 데이터와 ContentType 반환
            return response['Body'].read(), response.get('ContentType')
        except ClientError as e:
            logging.error(f"S3 객체 다운로드 실패: {e}")
            return None, None

    def upload_to_s3(self, key: str, data: Union[bytes, BinaryIO], content_type: str = 'image/jpeg'):
        """
        데이터를 S3에 업로드 (작은 파일용)

        Args:
            key (str): S3에 저장될 객체 이름 (경로 포함)
            data (Union[bytes, BinaryIO]): 업로드할 데이터 (바이트 또는 파일 객체)
            content_type (str): 콘텐츠 타입 (기본값: 'image/jpeg')

        Returns:
            bool: 업로드 성공 여부
        """
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type
            )
            return True

        except ClientError as e:
            logging.error(f"S3 업로드 실패: {e}")
            return False

    def upload_large_file(self, key: str, file_path: str, content_type: str = 'application/octet-stream'):
        """
        대용량 파일을 멀티파트 업로드로 S3에 업로드

        Args:
            key (str): S3에 저장될 객체 이름 (경로 포함)
            file_path (str): 업로드할 파일의 로컬 경로
            content_type (str): 콘텐츠 타입 (기본값: 'application/octet-stream')

        Returns:
            bool: 업로드 성공 여부
        """
        try:
            # 파일 크기 확인
            file_size = os.path.getsize(file_path)
            
            # 멀티파트 업로드 시작
            multipart_upload = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                ContentType=content_type
            )
            
            upload_id = multipart_upload['UploadId']
            parts = []
            
            # 파일을 청크로 나누어 업로드
            chunk_size = 10 * 1024 * 1024  # 10MB 청크
            part_number = 1
            
            with open(file_path, 'rb') as file:
                while True:
                    chunk = file.read(chunk_size)
                    if not chunk:
                        break
                    
                    # 청크 업로드
                    part = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=chunk
                    )
                    
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    
                    part_number += 1
            
            # 멀티파트 업로드 완료
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            return True
            
        except ClientError as e:
            logging.error(f"대용량 파일 S3 업로드 실패: {e}")
            # 업로드 중 오류 발생 시 멀티파트 업로드 취소 시도
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=key,
                    UploadId=upload_id
                )
            except:
                pass
            return False

def get_s3_client() -> S3Client:
    return S3Client()