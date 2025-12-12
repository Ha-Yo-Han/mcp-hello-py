# MCP Hello Server (Python)

간단한 Hello MCP 서버 - 이름을 입력받아 한국어로 인사합니다!

## 구조

```
mcp-hello-py/
├── src/
│   ├── __init__.py       # 패키지 초기화
│   └── server.py         # MCP 서버
├── .env                  # 환경 변수 설정
├── requirements.txt      # 의존성
├── pyproject.toml        # 프로젝트 메타데이터
├── Dockerfile            # Docker 설정
└── README.md             # 이 파일
```

## 주요 기능

- **say_hello**: "안녕하세요, {name}님!" 형식으로 인사
- **say_hello_multiple**: 여러 사람에게 한 번에 인사
- **MCP 프로토콜**: Tools, Resources, Prompts 지원

## 설치

```bash
# 가상 환경 생성 및 활성화
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt
```

## 환경 변수 설정

`.env` 파일을 생성합니다:

```bash
# .env 파일 내용
PORT=8080
```

## 실행

### Streamable HTTP 모드 (Cloud Run, Web)
```bash
python3 src/server.py --http-stream

# 커스텀 포트 사용
PORT=3000 python3 src/server.py --http-stream
```

## 아키텍처

```
┌─────────────┐       ┌─────────────────┐
│  Postman /  │ ───▶  │  MCP Hello      │
│  MCP Client │       │  Server         │
│             │ ◀───  │  (Python)       │
└─────────────┘       └─────────────────┘
     HTTP                  MCP
   POST /mcp             Protocol
```

## 테스트 (Postman)

### Headers 설정 (모든 요청에 필수)

| Header | Value |
|--------|-------|
| `Content-Type` | `application/json` |
| `Accept` | `application/json` |

### 1. MCP 서버 초기화

- **Method**: `POST`
- **URL**: `http://localhost:8080/mcp`
- **Body** (raw JSON):

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {"name": "postman", "version": "1.0.0"}
  }
}
```

### 2. Tool 목록 조회

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/list",
  "params": {}
}
```

### 3. say_hello 호출

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "say_hello",
    "arguments": {"name": "김철수"}
  }
}
```

**응답 예시:**
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "content": [{"type": "text", "text": "안녕하세요, 김철수님!"}]
  }
}
```

### 4. say_hello_multiple 호출

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "tools/call",
  "params": {
    "name": "say_hello_multiple",
    "arguments": {"names": ["김철수", "이영희", "박민수"]}
  }
}
```

**응답 예시:**
```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "result": {
    "content": [{"type": "text", "text": "• 안녕하세요, 김철수님!\n• 안녕하세요, 이영희님!\n• 안녕하세요, 박민수님!"}]
  }
}
```

## Cloud Run 배포

### 배포된 서버 테스트

배포 URL 예시: `https://mcp-hello-py-xxxxxx.asia-northeast3.run.app/mcp`

Postman에서 URL만 변경하여 동일하게 테스트 가능합니다.

### 환경 변수 설정 (Cloud Run)

| 변수 | 값 | 설명 |
|------|-----|------|
| `PORT` | `8080` | Cloud Run 기본 포트 (자동 설정됨) |

## MCP Tools

### say_hello

한 사람에게 인사합니다.

| 파라미터 | 타입 | 필수 | 설명 |
|---------|------|------|------|
| `name` | string | O | 인사할 사람의 이름 |

### say_hello_multiple

여러 사람에게 한 번에 인사합니다.

| 파라미터 | 타입 | 필수 | 설명 |
|---------|------|------|------|
| `names` | array | O | 이름 리스트 |

## 기술 스택

- **Python**: 3.11+
- **MCP SDK**: 1.23.0+ (FastMCP)
- **Pydantic**: 2.x
- **Uvicorn**: ASGI 서버
- **Docker**: 컨테이너화

## 전송 모드

| 모드 | 사용처 | 엔드포인트 |
|------|--------|-----------|
| stdio | Claude Desktop, MCP Inspector | stdin/stdout |
| Streamable HTTP | Cloud Run, Web | `POST /mcp` |

## 참고

- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [FastMCP 문서](https://github.com/modelcontextprotocol/python-sdk)

## 라이선스

MIT License
