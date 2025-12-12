#!/usr/bin/env python3
"""
MY MCP Server

이 모듈은 (광역시/특별시/세종시) 이름을 받아,
기상청 단기예보 조회서비스(2.0)의 초단기실황(getUltraSrtNcst)을 호출해
가장 최근 정각(데이터 없으면 -1시간)의 실황을 JSON으로 반환하는 MCP 서버입니다.

주요 기능:
        - 입력: 서울/부산/대구/인천/광주/대전/울산/세종 (광역 단위)
        - base_date: 당일(KST) 고정
        - base_time: 최근 정각(HH00) 사용
        - 데이터가 없으면: 같은 날짜 안에서만 -1시간 재조회(최대 2회)
        - 응답: category를 한글 키로 정규화 + 그룹핑(기온/습도/강수/바람)
        - PTY(강수형태) 코드값을 한글 설명으로 변환
        - VEC(풍향) 값을 16방위 한글로 변환

환경 변수:
        - KMA_SERVICE_KEY: 공공데이터포털 인증키(ServiceKey)
        - PORT: HTTP Stream 포트(기본값: 8080)

실행 예시:
        - stdio 모드:
                $ python src/server.py
        - HTTP Stream 모드:
                $ python src/server.py --http-stream
                $ PORT=3000 python src/server.py --http-stream
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# =============================================================================
# 상수/매핑
# =============================================================================

KST = ZoneInfo("Asia/Seoul")

# 기상청 초단기실황(getUltraSrtNcst) 엔드포인트
ULTRA_SRT_NCST_URL = (
    "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
)

# 광역(특별)시 대표 격자 좌표(nx, ny)
# - 광역 단위 데모/과제 목적의 "대표 지점"입니다. (도시 내부 지역별 오차는 감수)
CITY_TO_GRID: Dict[str, Tuple[int, int]] = {
    "서울": (60, 127),
    "부산": (98, 76),
    "대구": (89, 90),
    "인천": (55, 124),
    "광주": (58, 74),
    "대전": (67, 100),
    "울산": (102, 84),
    "세종": (66, 103),
}

# PTY(강수형태) 코드(초단기 기준)
PTY_MAP_ULTRA: Dict[int, str] = {
    0: "없음",
    1: "비",
    2: "비/눈",
    3: "눈",
    4: "소나기",  # 문서/데이터에 따라 포함될 수 있어 확장
    5: "빗방울",
    6: "빗방울/눈날림",
    7: "눈날림",
}

# 초단기실황 카테고리: 한글명/단위/그룹/형변환
CATEGORY_META: Dict[str, Dict[str, Any]] = {
    "T1H": {"ko": "기온", "unit": "℃", "group": "기온", "cast": float},
    "REH": {"ko": "습도", "unit": "%", "group": "습도", "cast": int},
    "RN1": {"ko": "1시간 강수량", "unit": "mm", "group": "강수", "cast": float},
    "PTY": {"ko": "강수형태", "unit": "코드", "group": "강수", "cast": int},
    "VEC": {"ko": "풍향", "unit": "deg", "group": "바람", "cast": int},
    "WSD": {"ko": "풍속", "unit": "m/s", "group": "바람", "cast": float},
    "UUU": {"ko": "동서바람성분", "unit": "m/s", "group": "바람", "cast": float},
    "VVV": {"ko": "남북바람성분", "unit": "m/s", "group": "바람", "cast": float},
}

# 풍향(deg) -> 16방위 한글
WIND_DIR_16_KO = [
    "북",
    "북북동",
    "북동",
    "동북동",
    "동",
    "동남동",
    "남동",
    "남남동",
    "남",
    "남남서",
    "남서",
    "서남서",
    "서",
    "서북서",
    "북서",
    "북북서",
]

# =============================================================================
# FastMCP 서버 생성
# =============================================================================

mcp = FastMCP(
    name="mcp-weather-forecast",
    instructions="광역시명을 받아 기상청 초단기실황(getUltraSrtNcst, JSON)을 조회해 사람이 읽기 좋은 형태로 반환하는 MCP 서버입니다.",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)

# =============================================================================
# 내부 유틸
# =============================================================================


def _need_env(name: str) -> str:
    """환경 변수가 비어 있으면 예외를 발생시킵니다."""
    v = (os.getenv(name) or "").strip()
    if not v:
        raise RuntimeError(f"{name} 환경변수가 비어 있습니다.")
    return v


def normalize_city(city: str) -> str:
    """
    도시 입력을 관용적으로 정규화합니다.
    예: '서울특별시' -> '서울', '부산광역시' -> '부산', '세종시' -> '세종'
    """
    if not city:
        return ""
    s = city.strip()
    for suffix in ("특별자치시", "특별시", "광역시", "시"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s.strip()


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def as_base_date(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def as_base_time_hour(d: datetime) -> str:
    # 정시 단위 "HH00"
    return f"{d.hour:02d}00"


def safe_cast(meta_cast: Any, raw: Any) -> Any:
    """
    obsrValue(문자열)를 적절한 숫자형으로 변환합니다.
    - 실패하면 원본(raw)을 유지합니다.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if s == "":
        return None

    try:
        if meta_cast is int:
            return int(float(s))
        if meta_cast is float:
            return float(s)
        return meta_cast(s)
    except Exception:
        return raw


def wind_deg_to_16dir_ko(deg: Optional[int]) -> Optional[str]:
    if deg is None:
        return None
    # 16방위: 22.5도 간격, 중앙값 보정(+11.25)
    idx = int(((deg % 360) + 11.25) // 22.5) % 16
    return WIND_DIR_16_KO[idx]


def extract_items(api_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """응답에서 items.item 리스트만 안전하게 꺼냅니다."""
    try:
        body = api_json.get("response", {}).get("body", {})
        item = body.get("items", {}).get("item", [])
        if isinstance(item, list):
            return item
        if isinstance(item, dict):
            return [item]
        return []
    except Exception:
        return []


def normalize_observations(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    카테고리 한글 변환 + 사람이 보기 좋은 그룹 키(기온/습도/강수/바람)로 묶습니다.
    """
    grouped: Dict[str, Dict[str, Any]] = {
        "기온": {},
        "습도": {},
        "강수": {},
        "바람": {},
        "기타": {},
    }

    for it in items:
        code = str(it.get("category", "")).strip()
        raw_val = it.get("obsrValue")

        meta = CATEGORY_META.get(code)
        if meta:
            name_ko = meta["ko"]
            unit = meta["unit"]
            group = meta["group"]
            val = safe_cast(meta.get("cast"), raw_val)
        else:
            name_ko = code or "UNKNOWN"
            unit = None
            group = "기타"
            val = raw_val

        entry: Dict[str, Any] = {
            "코드": code,
            "값": val,
            "단위": unit,
        }

        # PTY(강수형태) 코드 → 한글 설명
        if code == "PTY":
            pty_int = val if isinstance(val, int) else safe_cast(int, raw_val)
            if isinstance(pty_int, int):
                entry["설명"] = PTY_MAP_ULTRA.get(pty_int, f"알 수 없음({pty_int})")
            else:
                entry["설명"] = "알 수 없음"

        # VEC(풍향) → 16방위 한글
        if code == "VEC":
            vec_int = val if isinstance(val, int) else safe_cast(int, raw_val)
            if isinstance(vec_int, int):
                entry["16방위"] = wind_deg_to_16dir_ko(vec_int)

        if group not in grouped:
            group = "기타"

        # 사람이 보기 좋은 키: 한글 항목명
        key = name_ko
        if key in grouped[group]:
            key = f"{key}({code})"

        grouped[group][key] = entry

    return grouped


def fetch_ultra_srt_ncst(
    client: httpx.Client,
    service_key: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
) -> Dict[str, Any]:
    """
    httpx(httpx)로 기상청 초단기실황(getUltraSrtNcst)을 호출해 JSON(dict)을 반환합니다.
    """
    params = {
        "serviceKey": service_key,
        "numOfRows": "1000",
        "pageNo": "1",
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": str(nx),
        "ny": str(ny),
    }

    r = client.get(
        ULTRA_SRT_NCST_URL, params=params, headers={"Accept": "application/json"}
    )
    r.raise_for_status()
    return r.json()


# =============================================================================
# Tools (도구)
# =============================================================================


@mcp.tool()
def list_supported_cities() -> Dict[str, Any]:
    """
    지원하는 광역(특별)시 목록을 반환합니다.
    """
    return {
        "supported_cities": sorted(CITY_TO_GRID.keys()),
        "examples": ["서울", "서울특별시", "서울시", "부산", "부산광역시", "세종시"],
    }


@mcp.tool()
def get_now_weather(city: str) -> Dict[str, Any]:
    """
    광역시명을 입력받아, 오늘 날짜 기준 가장 최근 정각(데이터 없으면 -1시간)의 초단기실황을 조회합니다.

    규칙:
    - base_date: 당일(KST) 고정
    - base_time: 최근 정각(HH00)
    - items가 비어있으면: -1시간 재조회(단, 날짜가 바뀌면 재조회하지 않음)

    Returns:
            성공: ok=true + 그룹핑된 실황(JSON)
            실패: ok=false + 에러 메시지
    """
    try:
        service_key = _need_env("KMA_SERVICE_KEY")
    except Exception as e:
        return {"ok": False, "error": str(e)}

    city_norm = normalize_city(city)
    if city_norm not in CITY_TO_GRID:
        return {
            "ok": False,
            "error": "지원하지 않는 지역입니다.",
            "input": city,
            "normalized": city_norm,
            "supported_cities": sorted(CITY_TO_GRID.keys()),
        }

    nx, ny = CITY_TO_GRID[city_norm]
    now = now_kst()

    # base_date는 '당일'로 고정(요구사항)
    base_date = as_base_date(now)

    # 1차: 최근 정각, 2차: -1시간(동일 날짜만)
    hour_floor = now.replace(minute=0, second=0, microsecond=0)
    candidates: List[datetime] = [hour_floor, hour_floor - timedelta(hours=1)]

    attempts: List[Dict[str, Any]] = []
    last_error: Optional[str] = None

    with httpx.Client(timeout=8.0) as client:
        for cand_dt in candidates:
            # 날짜가 바뀌면(base_date 고정 요구사항) 재시도하지 않음
            if as_base_date(cand_dt) != base_date:
                continue

            base_time = as_base_time_hour(cand_dt)

            try:
                api_json = fetch_ultra_srt_ncst(
                    client=client,
                    service_key=service_key,
                    base_date=base_date,
                    base_time=base_time,
                    nx=nx,
                    ny=ny,
                )

                header = api_json.get("response", {}).get("header", {})
                result_code = str(header.get("resultCode", "")).strip()
                result_msg = str(header.get("resultMsg", "")).strip()

                items = extract_items(api_json)
                attempts.append(
                    {
                        "base_date": base_date,
                        "base_time": base_time,
                        "resultCode": result_code,
                        "resultMsg": result_msg,
                        "count": len(items),
                    }
                )

                # API 자체가 정상(00) + items 존재하면 성공 처리
                if result_code == "00" and items:
                    # baseDate/baseTime/nx/ny는 item에도 있으나, 우리가 쓰는 값으로 고정해 반환
                    return {
                        "ok": True,
                        "지역": city_norm,
                        "격자": {"nx": nx, "ny": ny},
                        "발표": {"base_date": base_date, "base_time": base_time},
                        "실황": normalize_observations(items),
                        "attempts": attempts,
                    }

                last_error = f"정상 응답이지만 데이터가 비어있습니다(resultCode={result_code}, count={len(items)})"

            except httpx.HTTPStatusError as e:
                last_error = f"HTTPStatusError: {e}"
            except httpx.RequestError as e:
                last_error = f"RequestError: {e}"
            except Exception as e:
                last_error = f"Exception: {e}"

    return {
        "ok": False,
        "error": last_error or "알 수 없는 오류",
        "지역": city_norm,
        "격자": {"nx": nx, "ny": ny},
        "발표": {
            "base_date": base_date,
            "candidate_times": [
                as_base_time_hour(c) for c in candidates if as_base_date(c) == base_date
            ],
        },
        "attempts": attempts,
    }


# =============================================================================
# Resources (리소스)
# =============================================================================


@mcp.resource("docs://weather/readme")
def get_readme() -> str:
    """
    서버 사용 가이드를 제공합니다.
    """
    return """# MCP Weather Nowcast Server (KMA)

## 환경변수
- KMA_SERVICE_KEY: 공공데이터포털 인증키(ServiceKey)
- PORT: HTTP Stream 포트(기본 8080)

## Tools
### list_supported_cities
- 지원 도시 목록을 반환합니다.

### get_now_weather
- 입력: city (예: 서울/부산/대구/인천/광주/대전/울산/세종)
- 규칙:
  - base_date: 당일(KST) 고정
  - base_time: 최근 정각(HH00) -> 데이터 없으면 -1시간(동일 날짜만)
- 출력:
  - 실황: 기온/습도/강수/바람 그룹으로 보기 좋게 묶은 JSON
  - PTY(강수형태) 및 VEC(풍향)는 한글 설명(text) 포함
"""


# =============================================================================
# Prompts (프롬프트)
# =============================================================================


@mcp.prompt()
def nowcast_briefing(city: str) -> str:
    """
    브리핑 요약 프롬프트 템플릿(선택)
    """
    return f"""당신은 기상 브리핑을 만드는 도우미입니다.
MCP tool `get_now_weather` 결과(JSON)를 읽고, '{city}'의 현재 날씨를 3-5문장으로 요약하세요.

포함할 내용:
- 기온(기온), 습도(습도)
- 강수형태(강수형태 설명)와 1시간 강수량(1시간 강수량)
- 풍향(풍향 16방위)과 풍속(풍속)
원본에 없는 값은 추정하지 말고 '데이터에 없음'이라고 답하세요.
"""


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    """
    MCP 서버 메인 진입점입니다.

    커맨드라인 인자:
    - --http-stream: HTTP Stream 모드
    - 기본값: stdio 모드

    환경 변수:
    - PORT: HTTP 서버 포트(기본값: 8080)
    """
    import sys

    if "--http-stream" in sys.argv:
        port = int(os.environ.get("PORT", 8080))
        mcp.settings.port = port
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
