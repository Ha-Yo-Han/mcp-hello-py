# MCP Hello Server (Python)

간단한 Hello MCP 서버 - 이름을 입력받아 한국어로 인사합니다!

## 🎯 주요 기능

- **단순 인사**: "안녕하세요, {name}님!" 형식
- **복수 인사**: 여러 사람에게 한 번에 인사
- **MCP 프로토콜**: Tools, Resources, Prompts 지원

## 📦 설치

```bash
# 가상 환경 생성 및 활성화, 의존성 설치 (python 사용)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 또는 python3 사용 (macOS/Linux 권장)
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

## 🚀 실행

### stdio 모드 (Claude Desktop)
```bash
# MCP 서버 시작 (표준 입출력 방식)
python src/server.py

# 또는 python3 사용
python3 src/server.py
```

### Streamable HTTP 모드 (Cloud Run, Web) - 권장
```bash
# Streamable HTTP 서버 시작
python src/server.py --http-stream

# 또는 python3 사용
python3 src/server.py --http-stream

# 기본 포트: 8890
# Endpoint: http://0.0.0.0:8890/mcp

# 커스텀 포트 사용
PORT=3000 python src/server.py --http-stream
PORT=3000 python3 src/server.py --http-stream
```

## 🔧 MCP Tools

### 1. say_hello
한 사람에게 인사합니다.

**파라미터:**
- `name` (string, 필수): 인사할 사람의 이름

**예시:**
```json
{"name": "김철수"}
```

**결과:**
```
안녕하세요, 김철수님!
```

### 2. say_hello_multiple
여러 사람에게 한 번에 인사합니다.

**파라미터:**
- `names` (array, 필수): 이름 리스트

**예시:**
```json
{"names": ["김철수", "이영희", "박민수"]}
```

**결과:**
```
• 안녕하세요, 김철수님!
• 안녕하세요, 이영희님!
• 안녕하세요, 박민수님!
```

## 🧪 테스트

```bash
# 단일 인사 테스트
python -c "from src.server import say_hello; print(say_hello('김철수'))"
# 또는
python3 -c "from src.server import say_hello; print(say_hello('김철수'))"

# 복수 인사 테스트
python -c "from src.server import say_hello_multiple; print(say_hello_multiple(['김철수', '이영희']))"
# 또는
python3 -c "from src.server import say_hello_multiple; print(say_hello_multiple(['김철수', '이영희']))"
```

## 🚀 GCP 배포

```bash
# 1. Artifact Registry 생성
gcloud artifacts repositories create mcp-hello-py \
  --repository-format=docker \
  --location=asia-northeast3

# 2. 배포
gcloud builds submit --config=cloudbuild.yaml
```

## 📁 프로젝트 구조

```
mcp-hello-py/
├── src/
│   ├── __init__.py       # 패키지 초기화
│   └── server.py         # MCP 서버 (~230줄)
├── requirements.txt      # 의존성
├── pyproject.toml       # 프로젝트 메타데이터
├── Dockerfile           # Docker 설정
├── cloudbuild.yaml      # GCP 배포 설정
└── README.md           # 이 파일
```

## 🛠️ 기술 스택

- **Python**: 3.11+
- **MCP SDK**: 1.23.0+ (FastMCP)
- **Pydantic**: 2.x
- **Uvicorn**: ASGI 서버
- **Docker**: 컨테이너화
- **GCP Cloud Run**: 배포

## 🌐 전송 모드

### 1. stdio (표준 입출력)
- **사용처**: Claude Desktop, MCP Inspector
- **장점**: 로컬 개발에 간편
- **통신**: stdin/stdout을 통한 JSON-RPC

### 2. Streamable HTTP (권장)
- **사용처**: Cloud Run, Lambda, 웹 서비스
- **장점**: HTTP 프로토콜, 확장 가능, Stateless 지원
- **엔드포인트**: `POST /mcp`

## 📄 라이선스

MIT License
