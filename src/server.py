#!/usr/bin/env python3
"""
Simple Hello MCP Server

이 모듈은 이름을 받아 한국어로 인사하는 간단한 MCP 서버를 제공합니다.

주요 기능:
    - 단일 인사: 한 사람에게 "안녕하세요, {name}님!" 형태로 인사
    - 복수 인사: 여러 사람에게 한 번에 인사
    - MCP 프로토콜: Tools, Resources, Prompts 지원
    - Streamable HTTP 전송: Cloud Run 등 서버리스 환경 지원

사용 예시:
    HTTP 모드 실행:
        $ python src/server.py --http-stream
        -> http://localhost:8080/mcp
    
    stdio 모드 실행:
        $ python src/server.py
    
    도구 호출:
        {"name": "say_hello", "arguments": {"name": "김철수"}}
        -> "안녕하세요, 김철수님!"

작성자: MCP Hello Team
버전: 1.0.0
라이선스: MIT
"""

import os

from mcp.server.fastmcp import FastMCP

# ============================================================================
# FastMCP 서버 생성
# ============================================================================

mcp = FastMCP(
    name="mcp-hello",
    instructions="이름을 받아 한국어로 인사하는 간단한 MCP 서버입니다.",
    stateless_http=True,
    json_response=True,
)

# Cloud Run 환경에서 Host 헤더 검증 비활성화
os.environ.setdefault("MCP_ALLOW_ORIGIN", "*")


# ============================================================================
# Tools (도구)
# ============================================================================

@mcp.tool()
def say_hello(name: str) -> str:
    """
    이름을 받아 한국어로 인사합니다.
    
    간단한 형식의 인사말을 반환하며, 이름이 비어있을 경우 기본 메시지를 반환합니다.
    
    Args:
        name: 인사할 대상의 이름 (예: "김철수", "이영희", "박민수")
    
    Returns:
        "안녕하세요, {name}님!" 형태의 인사말 문자열
    
    Examples:
        >>> say_hello("김철수")
        '안녕하세요, 김철수님!'
    """
    if not name or name.strip() == "":
        return "안녕하세요!"
    
    return f"안녕하세요, {name}님!"


@mcp.tool()
def say_hello_multiple(names: list[str]) -> str:
    """
    여러 사람에게 한 번에 인사합니다.
    
    이름 리스트를 받아 각 이름마다 인사말을 생성하고,
    불릿 포인트(•)로 구분하여 하나의 문자열로 반환합니다.
    
    Args:
        names: 인사할 대상의 이름 리스트 (예: ["김철수", "이영희", "박민수"])
    
    Returns:
        각 인사말이 줄바꿈으로 구분된 문자열
    
    Examples:
        >>> say_hello_multiple(["김철수", "이영희"])
        '• 안녕하세요, 김철수님!\\n• 안녕하세요, 이영희님!'
    """
    if not names:
        return ""
    
    greetings = []
    for name in names:
        greeting = say_hello(name)
        greetings.append(f"• {greeting}")
    
    return "\n".join(greetings)


# ============================================================================
# Resources (리소스)
# ============================================================================

@mcp.resource("docs://hello/readme")
def get_readme() -> str:
    """
    Hello MCP 서버 사용 가이드를 제공합니다.
    
    Returns:
        Markdown 형식의 문서
    """
    return """# Hello MCP Server Documentation

## 개요
이름을 받아 한국어로 인사하는 간단한 MCP 서버입니다.

## 사용 가능한 도구

### say_hello
한 사람에게 인사합니다.

**파라미터:**
- `name` (string, 필수): 인사할 사람의 이름

**예시:**
```json
{
  "name": "김철수"
}
```

**결과:**
```
안녕하세요, 김철수님!
```

### say_hello_multiple
여러 사람에게 한 번에 인사합니다.

**파라미터:**
- `names` (array, 필수): 인사할 사람들의 이름 리스트

**예시:**
```json
{
  "names": ["김철수", "이영희", "박민수"]
}
```

**결과:**
```
• 안녕하세요, 김철수님!
• 안녕하세요, 이영희님!
• 안녕하세요, 박민수님!
```

## 사용 방법

1. MCP 클라이언트에서 서버 연결
2. `say_hello` 또는 `say_hello_multiple` 도구 호출
3. 인사말 결과 확인

## 기술 스택
- Python 3.11+
- MCP Python SDK (FastMCP)
- Pydantic (타입 검증)
- Starlette + Uvicorn (HTTP Stream)
"""


# ============================================================================
# Prompts (프롬프트)
# ============================================================================

@mcp.prompt()
def greeting_message(recipient: str) -> str:
    """
    인사 메시지 작성 템플릿을 제공합니다.
    
    Args:
        recipient: 인사할 대상의 이름
    
    Returns:
        AI 어시스턴트를 위한 프롬프트 템플릿
    """
    greeting = say_hello(recipient)
    
    return f"""{recipient}님에게 보낼 인사 메시지를 작성하세요.

다음 형식으로 시작하세요:
{greeting}

메시지에 포함할 내용:
1. 따뜻한 인사
2. 간단한 소개 또는 목적
3. 정중한 마무리

톤: 친근하고 공손하게
길이: 3-5 문장
"""


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """
    MCP 서버의 메인 진입점입니다.
    
    커맨드라인 인자를 통해 전송 모드를 선택합니다:
    - --http-stream: HTTP Stream 모드
    - 기본값: stdio 모드 (표준 입출력)
    
    환경 변수:
        PORT: HTTP 서버 포트 (기본값: 8080)
    
    사용 예시:
        HTTP 모드:
            $ python src/server.py --http-stream
            $ PORT=3000 python src/server.py --http-stream
        
        stdio 모드:
            $ python src/server.py
    """
    import sys
    
    if "--http-stream" in sys.argv:
        import uvicorn
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.middleware.trustedhost import TrustedHostMiddleware
        from starlette.routing import Mount
        
        port = int(os.environ.get("PORT", 8080))
        
        app = Starlette(
            routes=[
                Mount("/", app=mcp.streamable_http_app()),
            ],
            middleware=[
                Middleware(TrustedHostMiddleware, allowed_hosts=["*"]),
            ],
        )
        
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
