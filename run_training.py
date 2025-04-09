import subprocess
import os
import time
import sys
import argparse
from datetime import datetime

def run_training_process(config_file, project_path=None, gpu_id="0"):
    """
    특정 구성 파일을 사용하여 학습 프로세스를 실행합니다.
    
    Args:
        config_file: 학습 구성 파일 경로
        project_path: 프로젝트 루트 경로 (기본값: None)
        gpu_id: 사용할 GPU ID (기본값: "0")
    """
    # 프로젝트 경로가 제공되지 않은 경우 현재 디렉토리 사용
    if project_path is None:
        project_path = os.getcwd()
    
    # 로그 디렉토리 설정
    log_dir = os.path.join(project_path, "training_logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # 로그 파일 이름 생성 (타임스탬프 포함)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file = os.path.join(log_dir, f"training-{timestamp}.log")
    
    # 가상 환경 및 스크립트 경로 설정
    venv_path = os.path.join(project_path, "venv/bin")
    sd_scripts_path = os.path.join(project_path, "sd-scripts")
    
    # 전체 명령어 구성
    training_command = (
        # f"source {venv_path}/activate && "
        f"{venv_path}/accelerate launch "
        f"--dynamo_backend no --dynamo_mode default "
        f"--gpu_ids {gpu_id} --mixed_precision bf16 "
        f"--num_processes 1 --num_machines 1 "
        f"--num_cpu_threads_per_process 2 "
        f"{sd_scripts_path}/flux_train_network.py "
        f"--config_file {config_file}"
    )
    
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
                cwd=project_path  # 작업 디렉토리 설정
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
                hours, remainder = divmod(duration, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                print(f"\nTraining completed with exit code: {process.returncode}")
                print(f"Total duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")
                
                # 로그 파일에 완료 정보 추가
                with open(log_file, 'a') as f:
                    f.write(f"\n=== Training completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                    f.write(f"Exit code: {process.returncode}\n")
                    f.write(f"Duration: {int(hours)}h {int(minutes)}m {int(seconds)}s\n")
                
                return process.returncode == 0
                
            except KeyboardInterrupt:
                print("\nScript interrupted, but training process continues in background.")
                print(f"Log file: {log_file}")
                print(f"Process PID: {process.pid}")
                return None
                
        except Exception as e:
            print(f"Error starting training process: {str(e)}")
            with open(log_file, 'a') as f:
                f.write(f"\n=== ERROR: {str(e)} ===\n")
            return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run training process with Kohya SS")
    parser.add_argument("--config", required=True, help="Path to the config file")
    parser.add_argument("--project_path", default=None, help="Path to the project root directory")
    parser.add_argument("--gpu", default="0", help="GPU ID to use")

    args = parser.parse_args()
    
    # 학습 프로세스 실행
    success = run_training_process(
        config_file=args.config,
        project_path=args.project_path,
        gpu_id=args.gpu
    )
    print("project path :", args.project_path)
    
    # 종료 코드 설정
    if success is None:
        # 사용자가 중단했지만 프로세스는 계속 실행 중
        sys.exit(0)
    elif success:
        # 성공적으로 완료
        sys.exit(0)
    else:
        # 오류 발생
        sys.exit(1)