# MCP Hello Server - Docker 이미지 빌드 설정
# 빌드: docker build -t mcp-hello-py .
# stdio 실행: docker run -it mcp-hello-py
# HTTP 실행: docker run -p 8890:8890 mcp-hello-py

# Python 3.11 Slim 이미지 (경량화)
FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /app

# 의존성 설치 (캐싱 최적화)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY . .

# HTTP Stream 포트 노출
EXPOSE 8890

# 환경 변수 설정 (기본값)
ENV PORT=8890

# 서버 시작 (기본: HTTP Stream 모드)
CMD ["python", "src/server.py", "--http-stream"]
