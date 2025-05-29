#!/bin/bash
# 1. 초기 도커 빌드
docker build -t ehemo-app-training:init -f Dockerfile-prod .

# 2. setup.sh 실행
echo "Setup 단계를 시작합니다..."
docker run --gpus all --name init_container ehemo-app-training:init || true

echo "Setup이 완료되었습니다. 이제 GUI 서비스를 시작합니다..."

# 3. gui.sh 실행 (같은 컨테이너에서)
echo "GUI 서비스가 준비되면 Ctrl+C를 눌러주세요..."
docker start init_container
docker exec -it init_container /bin/bash -c "./gui.sh" || true

# 4. 컨테이너를 새 이미지로 커밋 (CMD 포함)
docker commit --change='CMD ["/bin/bash", "-c", "source venv/bin/activate && python -m ehemo"]' init_container radiantjade/ehemo-app-training:final

# 5. 초기 컨테이너 정리
docker stop init_container
docker rm init_container
docker rmi ehemo-app-training:init

# 6. 이미지 푸시
docker push radiantjade/ehemo-app-training:final
