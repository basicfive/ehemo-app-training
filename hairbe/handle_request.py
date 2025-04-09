from pydantic import BaseModel
import time
import os
import json
import subprocess
from typing import Tuple, List
from datetime import datetime
from enum import Enum, auto
from hairbe.config import training_config
from hairbe.mq.mq_config import rabbit_mq_config
from hairbe.s3.s3_service import S3Client
from hairbe.mq.mq_service import RabbitMQService
from hairbe.dto.training import UserFaceTrainingJobMQConsumeMessage, UserFaceTrainingJobMQPublishMessage, Gender
import toml
from hairbe.hairbe_utils import get_extension_from_content_type

class RequestHandler:
    def __init__(
        self,
        s3_client: S3Client,
        message_publisher: RabbitMQService,
    ):
        self.s3_client = s3_client
        self.message_publisher = message_publisher

    def _publish_message(self, message: UserFaceTrainingJobMQPublishMessage):
        """상태 업데이트를 MQ로 전송"""
        message_json = message.model_dump_json()
        try:
            self.message_publisher.publish_message(
                publish_queue=rabbit_mq_config.RABBITMQ_TRAINING_PUBLISH,
                message=message_json.encode('utf-8')
            )
        except Exception as e:
            print(f"Error sending status update: {str(e)}")
            # 연결 재시도
            try:
                self.message_publisher.connect()
                # 재연결 후 다시 시도
                self.message_publisher.publish_message(
                    publish_queue=rabbit_mq_config.RABBITMQ_TRAINING_PUBLISH,
                    message=message_json.encode('utf-8')
                )
            except Exception as reconnect_error:
                print(f"Failed to reconnect and send status: {str(reconnect_error)}")

    def _upload_model_to_s3(self, file_path: str, s3_key: str, retry_count: int = 0) -> bool:
        print(f"모델 파일 S3 업로드 시도: {file_path} -> {s3_key}")

        upload_success = False

        for i in range(retry_count):
            try:
                # 대용량 파일 업로드 함수 호출
                upload_success = self.s3_client.upload_large_file(
                    key=s3_key,
                    file_path=file_path,
                )
                if upload_success:
                    break
                else:
                    print(f"모델 파일 S3 업로드 실패({retry_count})")
                    time.sleep(5)
            except Exception as e:
                print(f"모델 파일 S3 업로드 중 오류 발생({retry_count}): {str(e)}")
                time.sleep(5)

        print(f"모델 파일 S3 업로드 {'성공' if upload_success else '실패'}: {s3_key}")
        return upload_success

    def _update_training_config_file(self, lora_model_name: str) -> None:
        """학습 설정 파일(TOML)을 업데이트하는 함수"""
        config_file_path = training_config.config_file_path
        try:
            with open(config_file_path, 'r') as f:
                config_data = toml.load(f)
            
            # 기존 설정 중 필요한 값 업데이트
            # 모델 경로 설정
            config_data['ae'] = training_config.vae_path
            config_data['clip_l'] = training_config.clip_path
            config_data['t5xxl'] = training_config.t5xxl_path
            config_data['pretrained_model_name_or_path'] = training_config.base_model_path
            config_data['sample_prompts'] = training_config.sample_prompt_file_path
            
            # 훈련 관련 경로 설정
            config_data['train_data_dir'] = training_config.train_data_dir_path
            config_data['logging_dir'] = training_config.train_logging_dir
            config_data['output_dir'] = training_config.output_dir_path
            
            # 출력 모델 이름 설정
            config_data['output_name'] = lora_model_name
            config_data['wandb_run_name'] = lora_model_name
            
            # 수정된 설정을 TOML 파일로 저장
            with open(config_file_path, 'w') as f:
                toml.dump(config_data, f)
            
            print(f"학습 설정 파일이 성공적으로 업데이트되었습니다: {config_file_path}")
            
        except Exception as e:
            print(f"학습 설정 파일 업데이트 중 오류 발생: {str(e)}")
            raise

    def _set_train_dir(self, gender: Gender) -> str:
        # train 디렉토리 세팅
        if os.path.exists(training_config.train_dir_path):
            import shutil
            print(f"기존 train 디렉토리 삭제: {training_config.train_dir_path}")
            shutil.rmtree(training_config.train_dir_path)
        
        # train 디렉토리와 하위 디렉토리 생성
        print(f"train 디렉토리 생성: {training_config.train_dir_path}")
        os.makedirs(training_config.train_dir_path, exist_ok=True)
        os.makedirs(training_config.train_data_dir_path, exist_ok=True)
        os.makedirs(training_config.train_logging_dir, exist_ok=True)
        os.makedirs(training_config.output_dir_path, exist_ok=True)
        
        # sample prompt 디렉토리 및 빈 파일 생성
        os.makedirs(os.path.dirname(training_config.sample_prompt_file_path), exist_ok=True)
        with open(training_config.sample_prompt_file_path, 'w') as f:
            pass

        train_repeat_count = 1
        trigger_word = "sks"
        class_word = "man" if gender == Gender.MALE else "woman"
        image_dir_name = f"{train_repeat_count}_{trigger_word} {class_word}"

        train_image_dir_path = os.path.join(training_config.train_data_dir_path, image_dir_name)
        os.makedirs(train_image_dir_path, exist_ok=True)

        return train_image_dir_path

    def _download_images(self, image_dir_path: str, s3_key_list: List[str]):
        # 유저 제공한 이미지 다운로드
        for s3_key in s3_key_list:
            data, content_type = self.s3_client.download_from_s3(s3_key)
            if data:
                # 파일 이름 추출 및 확장자 결정
                base_filename, _ = os.path.splitext(os.path.basename(s3_key))
                extension = get_extension_from_content_type(content_type)
                
                # 파일 저장
                output_filepath = os.path.join(image_dir_path, f"{base_filename}{extension}")
                with open(output_filepath, 'wb') as f:
                    f.write(data)
                print(f"이미지 저장 완료: {output_filepath}")
            else:
                print(f"이미지 다운로드 실패: {s3_key}")
                raise Exception(f"이미지 다운로드 실패: {s3_key}")
        
    def _build_command(self, lora_model_name: str):
        # config file 세팅
        self._update_training_config_file(lora_model_name=lora_model_name)
        
        # 전체 명령어 구성
        training_command = (
            f"{training_config.project_path}/venv/bin/accelerate launch "
            f"--dynamo_backend no --dynamo_mode default "
            f"--gpu_ids {training_config.gpu_id} --mixed_precision bf16 "
            f"--num_processes 1 --num_machines 1 "
            f"--num_cpu_threads_per_process 2 "
            f"{training_config.sd_scripts_path}/flux_train_network.py "
            f"--config_file {training_config.config_file_path}"
        )
        print("training command : ", training_command)
        return training_command


    def _process_training_request(self, message: UserFaceTrainingJobMQConsumeMessage) -> int:
        start_time = time.time()

        # 학습 디렉토리 세팅
        image_dir_path = self._set_train_dir(gender=message.gender)

        # 이미지 다운로드
        self._download_images(image_dir_path=image_dir_path, s3_key_list=message.s3_key_list)

        # 학습 명령어 구성
        training_command = self._build_command(lora_model_name=message.lora_model_name)

        # 학습 시작
        is_success, training_time_sec = self._start_training(training_command)

        if not is_success:
            print(f"학습 실패: {message.user_face_training_job_id} {training_time_sec}초")
            raise Exception("학습 실패")

        # 모델 파일을 S3에 업로드
        model_file_path = os.path.join(training_config.output_dir_path, f"{message.lora_model_name}.safetensors")
        self._upload_model_to_s3(model_file_path, message.model_s3_key, retry_count=3)

        # s3 업로드 시간까지 포함한 전체 시간
        return int(time.time() - start_time)


    def handle_training_request(self, ch, method, properties, body: bytes):
        dict_data = json.loads(body)
        message = UserFaceTrainingJobMQConsumeMessage(**dict_data)
        try:
            whole_process_time_sec = self._process_training_request(message)
            is_success = True
        except Exception as e:
            print(f"학습 요청 실패: {e}")
            whole_process_time_sec = 0
            is_success = False
        finally:
            self._publish_message(
                message=UserFaceTrainingJobMQPublishMessage(
                    is_success=is_success,
                    user_face_training_job_id=message.user_face_training_job_id,
                    model_s3_key=message.model_s3_key,
                    lora_model_name=message.lora_model_name,
                    actual_training_time_sec=whole_process_time_sec,
                )
            )


    def _start_training(self, training_command: str) -> Tuple[bool, int]:
        try:
            # 로그 디렉토리 설정
            os.makedirs(training_config.subprocess_log_dir_path, exist_ok=True)
            
            # 로그 파일 이름 생성 (타임스탬프 포함)
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            log_file = os.path.join(training_config.subprocess_log_dir_path, f"training-{timestamp}.log")

            print(f"Starting training process...")
            print(f"Command: {training_command}")
            print(f"Log file: {log_file}")
            
            start_time = time.time()
            
            # 프로세스 실행
            with open(log_file, 'w') as f:
                # 실행 정보 기록
                f.write(f"=== Training started at {timestamp} ===\n")
                f.write(f"Command: {training_command}\n\n")
                f.flush()
                
                try:
                    # 명령어 실행
                    process = subprocess.Popen(
                        training_command,
                        shell=True,
                        stdout=f,
                        stderr=subprocess.STDOUT,
                        cwd=training_config.project_path  # 작업 디렉토리 설정
                    )
                    
                    # 로그 모니터링 스레드 대신 간단한 진행 표시
                    print("Training in progress. Press Ctrl+C to abort (process will continue in background).")
                    print(f"You can monitor the log with: tail -f {log_file}")
                    
                    try:
                        # 프로세스가 완료될 때까지 대기
                        process.wait()
                        
                        # 실행 시간 계산
                        end_time = time.time()
                        duration = end_time - start_time
                        actual_training_time_sec = int(duration)
                        hours, remainder = divmod(duration, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        
                        print(f"\nTraining completed with exit code: {process.returncode}")
                        print(f"Total duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")
                        
                        # 로그 파일에 완료 정보 추가
                        with open(log_file, 'a') as f:
                            f.write(f"\n=== Training completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                            f.write(f"Exit code: {process.returncode}\n")
                            f.write(f"Duration: {int(hours)}h {int(minutes)}m {int(seconds)}s\n")
                        
                        # 성공 여부와 학습 시간을 튜플로 반환
                        return process.returncode == 0, actual_training_time_sec
                        
                    except KeyboardInterrupt:
                        # 서브프로세스를 종료합니다
                        try:
                            print("\nKeyboard interrupt detected. Terminating training process...")
                            process.terminate()  # 먼저 SIGTERM으로 종료 시도
                            
                            # 프로세스가 종료될 때까지 일정 시간 대기(최대 5초)
                            for _ in range(5):
                                if process.poll() is not None:  # 프로세스가 종료되었는지 확인
                                    break
                                time.sleep(1)
                            
                            # 아직 종료되지 않았다면 강제 종료
                            if process.poll() is None:
                                print("Process did not terminate gracefully, forcing kill...")
                                process.kill()  # SIGKILL로 강제 종료
                            
                            # 종료 상태 확인
                            process.wait()
                            print(f"Process terminated with exit code: {process.returncode}")
                            
                            # 로그 파일에 중단 정보 추가
                            with open(log_file, 'a') as f:
                                f.write(f"\n=== Training interrupted and terminated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                                f.write(f"Exit code: {process.returncode}\n")
                        except Exception as kill_error:
                            print(f"Error while terminating process: {str(kill_error)}")
                        
                        # 인터럽트로 인한 학습 중단은 실패로 간주
                        actual_training_time_sec = int(time.time() - start_time)
                        return False, actual_training_time_sec
                        
                except Exception as e:
                    print(f"Error starting training process: {str(e)}")
                    with open(log_file, 'a') as f:
                        f.write(f"\n=== ERROR: {str(e)} ===\n")
                    # 프로세스 시작 중 오류 발생 시 실패로 간주하고 현재까지 소요된 시간 반환
                    actual_training_time_sec = int(time.time() - start_time)
                    return False, actual_training_time_sec
                    
        except Exception as e:
            print(f"Error in start_training: {str(e)}")
            # 함수 실행 중 예외 발생 시 실패로 간주하고 0초 반환 (훈련이 시작되지 않았으므로)
            return False, 0

