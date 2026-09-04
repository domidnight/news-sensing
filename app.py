import os
import re
import hmac
import hashlib
import json
import urllib.parse
import email.utils
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import feedparser
import requests
import streamlit as st
from bs4 import BeautifulSoup
from google import genai

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    func,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


# =========================================================
# 0. 기본 설정
# =========================================================

NEWS_CATEGORY_NAMES = ["AI", "국무부", "국방부", "텍사스", "관세"]
SOCIAL_CATEGORY_NAMES = [
    "소셜 (Helberg)",
    "소셜 (Rubio)",
    "소셜 (Hegseth)",
]
CATEGORY_NAMES = NEWS_CATEGORY_NAMES + SOCIAL_CATEGORY_NAMES

# 무료 공개 X 임베드 타임라인 기반 센싱 대상
# 각 인물은 공식 직책 계정과 공개 개인 계정을 함께 확인합니다.
SOCIAL_ACCOUNTS = {
    "소셜 (Helberg)": [
        {"handle": "UnderSecE", "label": "Jacob S. Helberg · 국무부 공식"},
        {"handle": "jacobhelberg", "label": "Jacob Helberg · 개인 공개"},
    ],
    "소셜 (Rubio)": [
        {"handle": "SecRubio", "label": "Marco Rubio · 국무장관 공식"},
        {"handle": "marcorubio", "label": "Marco Rubio · 개인 공개"},
    ],
    "소셜 (Hegseth)": [
        {"handle": "SecWar", "label": "Pete Hegseth · 장관 공식"},
        {"handle": "PeteHegseth", "label": "Pete Hegseth · 개인 공개"},
    ],
}
KST = timezone(timedelta(hours=9))

# Google News 검색 시 최근 몇 시간을 볼지 설정
SEARCH_LOOKBACK_HOURS = int(os.environ.get("SEARCH_LOOKBACK_HOURS", "48"))

# Gemini 모델명은 Railway 변수에서 바꿀 수 있음
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")

RSS_TIMEOUT_SECONDS = int(os.environ.get("RSS_TIMEOUT_SECONDS", "10"))
RSS_MAX_WORKERS = int(os.environ.get("RSS_MAX_WORKERS", "10"))

# =========================================================
# V3 Hybrid Collector 설정
# =========================================================

DIRECT_TIMEOUT_SECONDS = int(
    os.environ.get("DIRECT_TIMEOUT_SECONDS", "8")
)
DIRECT_MAX_WORKERS = int(
    os.environ.get("DIRECT_MAX_WORKERS", "14")
)
DIRECT_MAX_ARTICLES_PER_SOURCE = int(
    os.environ.get("DIRECT_MAX_ARTICLES_PER_SOURCE", "24")
)
DIRECT_SITEMAPS_PER_SOURCE = int(
    os.environ.get("DIRECT_SITEMAPS_PER_SOURCE", "6")
)
DIRECT_LOOKBACK_HOURS = int(
    os.environ.get(
        "DIRECT_LOOKBACK_HOURS",
        str(max(72, SEARCH_LOOKBACK_HOURS + 24)),
    )
)

# 아래 9개는 사용자가 설정 목록에 넣었을 때
# Google News + 공개 웹 직접 센싱을 동시에 수행합니다.
DIRECT_SOURCE_PROFILES = {
    "wsj.com": {
        "label": "The Wall Street Journal",
        "start_pages": [
            "https://www.wsj.com/",
            "https://www.wsj.com/tech",
            "https://www.wsj.com/politics",
        ],
    },
    "ft.com": {
        "label": "Financial Times",
        "start_pages": [
            "https://www.ft.com/",
            "https://www.ft.com/world",
            "https://www.ft.com/technology",
            "https://www.ft.com/us",
        ],
    },
    "bloomberg.com": {
        "label": "Bloomberg",
        "start_pages": [
            "https://www.bloomberg.com/",
            "https://www.bloomberg.com/technology",
            "https://www.bloomberg.com/politics",
        ],
    },
    "reuters.com": {
        "label": "Reuters",
        "start_pages": [
            "https://www.reuters.com/",
            "https://www.reuters.com/world/us/",
            "https://www.reuters.com/technology/",
            "https://www.reuters.com/business/",
            "https://www.reuters.com/legal/",
            "https://www.reuters.com/legal/government/",
        ],
    },
    "politico.com": {
        "label": "POLITICO",
        "start_pages": [
            "https://www.politico.com/",
            "https://www.politico.com/news",
        ],
    },
    "washingtonpost.com": {
        "label": "The Washington Post",
        "start_pages": [
            "https://www.washingtonpost.com/",
            "https://www.washingtonpost.com/politics/",
            "https://www.washingtonpost.com/technology/",
            "https://www.washingtonpost.com/business/",
        ],
    },
    "axios.com": {
        "label": "Axios",
        "start_pages": [
            "https://www.axios.com/",
            "https://www.axios.com/technology",
            "https://www.axios.com/politics-policy",
            "https://www.axios.com/economy-business",
        ],
    },
    "whitehouse.gov": {
        "label": "The White House",
        # V3.2.2: White House는 News 허브에 노출되는 콘텐츠만 센싱
        "start_pages": [
            "https://www.whitehouse.gov/news/",
        ],
        "listing_only": True,
        "allowed_path_prefixes": [
            "/releases/",
            "/briefings-statements/",
            "/presidential-actions/",
            "/fact-sheets/",
            "/remarks/",
            "/research/",
        ],
    },
    "state.gov": {
        "label": "U.S. Department of State",
        # V3.2.2: State Department는 Press Releases 목록만 센싱
        # 실제 개별 press release URL은 /releases/... 형태
        "start_pages": [
            "https://www.state.gov/press-releases/",
        ],
        "listing_only": True,
        "allowed_path_prefixes": [
            "/releases/",
        ],
    },
}

# Google News RSS가 site: 필터 밖 매체를 반환하는 경우를 막기 위한
# 공식/대표 source title 목록
GOOGLE_SOURCE_ALIASES = {
    "wsj.com": {
        "the wall street journal",
        "wall street journal",
        "wsj",
    },
    "ft.com": {
        "financial times",
        "ft",
    },
    "bloomberg.com": {
        "bloomberg",
        "bloomberg.com",
    },
    "reuters.com": {
        "reuters",
    },
    "politico.com": {
        "politico",
    },
    "washingtonpost.com": {
        "the washington post",
        "washington post",
    },
    "axios.com": {
        "axios",
    },
    "whitehouse.gov": {
        "the white house",
        "white house",
    },
    "state.gov": {
        "u.s. department of state",
        "us department of state",
        "department of state",
        "state department",
    },
}

COMMON_SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/news-sitemap.xml",
    "/sitemap-news.xml",
]


# V3.2: 각 사이트 자체 검색페이지를 키워드 단위로 보조 사용
NATIVE_SEARCH_TEMPLATES = {
    "reuters.com": {
        "label": "Reuters",
        "url": "https://www.reuters.com/site-search/?query={query}",
    },
    "wsj.com": {
        "label": "The Wall Street Journal",
        "url": "https://www.wsj.com/search?query={query}",
    },
    "ft.com": {
        "label": "Financial Times",
        "url": "https://www.ft.com/search?q={query}",
    },
    "bloomberg.com": {
        "label": "Bloomberg",
        "url": "https://www.bloomberg.com/search?query={query}",
    },
    "politico.com": {
        "label": "POLITICO",
        "url": "https://www.politico.com/search?q={query}",
    },
    "washingtonpost.com": {
        "label": "The Washington Post",
        "url": "https://www.washingtonpost.com/search/?query={query}",
    },
    "axios.com": {
        "label": "Axios",
        "url": "https://www.axios.com/search?q={query}",
    },
    "whitehouse.gov": {
        "label": "The White House",
        "url": "https://www.whitehouse.gov/news/?s={query}",
    },
    "state.gov": {
        "label": "U.S. Department of State",
        # State.gov 원문이 자동수집 서버에 403을 줄 때를 대비해
        # State Department의 Search.gov 검색 경로를 사용
        "url": (
            "https://findit.state.gov/search?"
            "query={query}&affiliate=dos_stategov&search="
        ),
    },
}

NATIVE_SEARCH_RESULTS_PER_KEYWORD = int(
    os.environ.get("NATIVE_SEARCH_RESULTS_PER_KEYWORD", "6")
)
NATIVE_SEARCH_MAX_ARTICLE_FETCHES = int(
    os.environ.get("NATIVE_SEARCH_MAX_ARTICLE_FETCHES", "180")
)

# Gemini 무료 한도 보호: 자동 10회 + 수동 10회
AUTO_SUMMARY_DAILY_LIMIT = int(os.environ.get("AUTO_SUMMARY_DAILY_LIMIT", "10"))
MANUAL_SUMMARY_DAILY_LIMIT = int(os.environ.get("MANUAL_SUMMARY_DAILY_LIMIT", "10"))
GEMINI_QUOTA_TZ = ZoneInfo("America/Los_Angeles")
_LAST_SUMMARY_ERROR = None

# 최초 1회만 DB에 들어가는 기본값.
# 이후에는 웹사이트의 관리자 설정 화면에서 수정/저장하면 DB 값이 계속 유지됩니다.
DEFAULT_KEYWORDS = {
    "AI": [
        "Trump AI",
        "AI Chips",
        "Anthropic",
        "OpenAI",
        "Bondi",
        "David Sacks",
        "AI Order",
        "Glasswing",
        "OpenAI TAC",
        "Trusted Access for Cyber",
        "AI Exports Program",
        "AI Exports",
        "AI Export",
        "Pax Silica",
    ],
    "국무부": [
        "State Department",
        "Department of State",
        "Secretary of State",
        "US Diplomacy",
    ],
    "국방부": [
        "Department of Defense",
        "Defense Department",
        "Pentagon",
        "US Military",
        "Defense Policy",
    ],
    "텍사스": [
        "Texas",
        "Texas Policy",
        "Texas Legislature",
        "Texas Economy",
    ],
    "관세": [
        "Tariff",
        "Tariffs",
        "Section 232",
        "Section 301",
        "Import Duty",
    ],
}

DEFAULT_DOMAINS = [
    "wsj.com",
    "ft.com",
    "bloomberg.com",
    "reuters.com",
    "politico.com",
    "washingtonpost.com",
    "axios.com",
]


# =========================================================
# 1. 데이터베이스
#    - Railway에서는 PostgreSQL의 DATABASE_URL 사용
#    - DATABASE_URL이 없으면 테스트용 SQLite 사용
# =========================================================

def _database_url():
    raw = os.environ.get("DATABASE_URL", "sqlite:///news_sensing.db")

    # Railway가 postgresql:// 또는 postgres:// 형태로 줄 수 있으므로
    # SQLAlchemy + psycopg 드라이버 형식으로 바꿉니다.
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+psycopg://", 1)
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw


DB_URL = _database_url()

if DB_URL.startswith("sqlite"):
    engine = create_engine(
        DB_URL,
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
else:
    engine = create_engine(DB_URL, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Category(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Keyword(Base):
    __tablename__ = "keywords"
    __table_args__ = (
        UniqueConstraint("category_id", "keyword", name="uq_category_keyword"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    category_id: Mapped[int] = mapped_column(
        ForeignKey("categories.id", ondelete="CASCADE"), nullable=False
    )
    keyword: Mapped[str] = mapped_column(String(300), nullable=False)


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    domain: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    url_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    link: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(String(255), nullable=False, default="Unknown")
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)


class ArticleMatch(Base):
    __tablename__ = "article_matches"
    __table_args__ = (
        UniqueConstraint(
            "article_id", "category_id", "keyword",
            name="uq_article_category_keyword"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    article_id: Mapped[int] = mapped_column(
        ForeignKey("articles.id", ondelete="CASCADE"), nullable=False
    )
    category_id: Mapped[int] = mapped_column(
        ForeignKey("categories.id", ondelete="CASCADE"), nullable=False
    )
    keyword: Mapped[str] = mapped_column(String(300), nullable=False)


class SummaryUsage(Base):
    __tablename__ = "summary_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    article_id: Mapped[int | None] = mapped_column(
        ForeignKey("articles.id", ondelete="SET NULL"), nullable=True
    )
    mode: Mapped[str] = mapped_column(String(20), nullable=False)  # auto / manual
    used_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def init_db():
    """
    DB 테이블을 만들고 필요한 기본 카테고리를 보장합니다.
    기존 운영 DB가 있어도 새 소셜 카테고리는 자동으로 추가됩니다.
    """
    Base.metadata.create_all(engine)

    with SessionLocal() as session:
        category_by_name = {
            c.name: c
            for c in session.scalars(select(Category)).all()
        }

        # 기존 DB에도 새 카테고리를 자동 추가
        for idx, category_name in enumerate(CATEGORY_NAMES):
            if category_name not in category_by_name:
                obj = Category(name=category_name, sort_order=idx)
                session.add(obj)
                session.flush()
                category_by_name[category_name] = obj
            else:
                category_by_name[category_name].sort_order = idx

        # 뉴스 카테고리 키워드는 해당 카테고리에 키워드가 하나도 없을 때만 기본값 입력
        for category_name in NEWS_CATEGORY_NAMES:
            category = category_by_name[category_name]
            kw_count = session.scalar(
                select(func.count(Keyword.id))
                .where(Keyword.category_id == category.id)
            ) or 0

            if kw_count == 0:
                for kw in DEFAULT_KEYWORDS.get(category_name, []):
                    session.add(
                        Keyword(
                            category_id=category.id,
                            keyword=normalize_keyword(kw),
                        )
                    )

        # 언론사 설정이 완전히 비어 있을 때만 기본값 입력
        source_count = session.scalar(select(func.count(Source.id))) or 0
        if source_count == 0:
            for domain in DEFAULT_DOMAINS:
                session.add(
                    Source(domain=normalize_domain(domain), enabled=True)
                )

        session.commit()


# =========================================================
# 2. 키워드 / 도메인 처리
# =========================================================

def normalize_keyword(value: str) -> str:
    """앞뒤 공백 제거 + 연속된 공백을 하나로 정리."""
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_domain(value: str) -> str:
    """https://, www., 뒤쪽 / 등을 제거해 site:검색용 도메인으로 정리."""
    value = (value or "").strip().lower()
    if not value:
        return ""

    if "://" not in value:
        value = "https://" + value

    parsed = urllib.parse.urlparse(value)
    domain = parsed.netloc or parsed.path
    domain = domain.split("@")[-1].split(":")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    return domain.strip("/")


def parse_multiline_values(text: str) -> list[str]:
    """
    한 줄에 하나를 권장하지만, 쉼표로 붙여 넣어도 동작하도록 처리합니다.
    예: AI Policy
        OpenAI
        Tariff
    """
    values = []
    seen = set()

    for part in re.split(r"[\n,]+", text or ""):
        item = normalize_keyword(part)
        if item and item.lower() not in seen:
            values.append(item)
            seen.add(item.lower())

    return values


# 검색어 마지막 단어의 자연스러운 단수/복수 처리를 위한 최소 규칙
# 뉴스/정책 분야에서 자주 쓰는 불규칙형도 포함합니다.
IRREGULAR_SINGULAR_TO_PLURAL = {
    "analysis": "analyses",
    "basis": "bases",
    "crisis": "crises",
    "criterion": "criteria",
    "index": "indices",
    "matrix": "matrices",
    "person": "people",
    "child": "children",
    "man": "men",
    "woman": "women",
}
IRREGULAR_PLURAL_TO_SINGULAR = {
    plural: singular
    for singular, plural in IRREGULAR_SINGULAR_TO_PLURAL.items()
}

# 자동 복수형을 만들지 않는 대표적인 불가산/고유 성격 단어
UNCOUNTABLE_WORDS = {
    "ai",
    "data",
    "information",
    "intelligence",
    "research",
    "news",
    "equipment",
    "software",
    "hardware",
    "media",
}


def _match_word_case(source: str, target_lower: str) -> str:
    """원래 단어의 대소문자 느낌을 최대한 유지합니다."""
    if source.isupper():
        return target_lower.upper()
    if source[:1].isupper():
        return target_lower[:1].upper() + target_lower[1:]
    return target_lower


def _pluralize_word(word: str) -> str | None:
    """
    단어 하나의 자연스러운 영어 복수형을 만듭니다.
    예: Policy -> Policies, Center -> Centers, Company -> Companies
    """
    word = (word or "").strip()
    if not word:
        return None

    lower = word.lower()

    # AI, OpenAI 같은 약어/고유표현과 대표적인 불가산명사는 자동 변화하지 않음
    if lower in UNCOUNTABLE_WORDS:
        return None
    if len(word) <= 2 and word.isupper():
        return None

    if lower in IRREGULAR_SINGULAR_TO_PLURAL:
        return _match_word_case(
            word,
            IRREGULAR_SINGULAR_TO_PLURAL[lower],
        )

    # 이미 대표적인 복수형처럼 보이면 새 복수형을 만들지 않음
    if lower in IRREGULAR_PLURAL_TO_SINGULAR:
        return None

    if re.search(r"[^aeiou]y$", lower):
        plural = lower[:-1] + "ies"
    elif re.search(r"(s|x|z|ch|sh)$", lower):
        plural = lower + "es"
    else:
        plural = lower + "s"

    return _match_word_case(word, plural)


def _singularize_word(word: str) -> str | None:
    """
    사용자가 복수형을 입력한 경우에도 대응되는 단수형을 함께 허용합니다.
    예: Policies -> Policy, Centers -> Center
    """
    word = (word or "").strip()
    if not word:
        return None

    lower = word.lower()

    if lower in UNCOUNTABLE_WORDS:
        return None

    if lower in IRREGULAR_PLURAL_TO_SINGULAR:
        return _match_word_case(
            word,
            IRREGULAR_PLURAL_TO_SINGULAR[lower],
        )

    singular = None

    if re.search(r"[^aeiou]ies$", lower) and len(lower) > 3:
        singular = lower[:-3] + "y"
    elif re.search(r"(ches|shes|xes|zes|sses)$", lower):
        singular = lower[:-2]
    elif lower.endswith("s") and not lower.endswith(
        ("ss", "us", "is")
    ):
        singular = lower[:-1]

    if not singular or singular == lower:
        return None

    return _match_word_case(word, singular)


def _strip_outer_quotes(value: str) -> str:
    """검색어 구성요소의 바깥 큰따옴표/작은따옴표를 제거합니다."""
    value = normalize_keyword(value)

    if len(value) >= 2:
        if (
            (value[0] == '"' and value[-1] == '"')
            or (value[0] == "'" and value[-1] == "'")
        ):
            value = value[1:-1].strip()

    return normalize_keyword(value)


def parse_keyword_expression(keyword: str) -> list[str]:
    """
    검색 문법:
    - AI Policy
      -> 하나의 phrase
    - "AI" AND "Data Center"
      -> AI와 Data Center(s)가 기사 안에 모두 있어야 함

    AND / And / and 모두 허용합니다.
    """
    keyword = normalize_keyword(keyword)
    if not keyword:
        return []

    parts = re.split(r"\s+AND\s+", keyword, flags=re.IGNORECASE)
    parts = [_strip_outer_quotes(part) for part in parts]
    return [part for part in parts if part]


def phrase_variants(phrase: str) -> list[str]:
    """
    phrase 전체는 그대로 유지하되 마지막 단어만 자연스러운
    단수/복수형을 자동 허용합니다.

    예:
    AI Policy -> AI Policy / AI Policies
    Data Center -> Data Center / Data Centers
    Data Centers -> Data Centers / Data Center
    """
    phrase = _strip_outer_quotes(phrase)
    if not phrase:
        return []

    words = phrase.split()
    if not words:
        return []

    variants = [phrase]
    last_word = words[-1]

    alternate_last = _singularize_word(last_word)
    if alternate_last is None:
        alternate_last = _pluralize_word(last_word)

    if alternate_last and alternate_last.lower() != last_word.lower():
        alternate_phrase = " ".join(words[:-1] + [alternate_last])
        variants.append(alternate_phrase)

    # 순서 유지 + 중복 제거
    result = []
    seen = set()

    for variant in variants:
        key = variant.lower()
        if key not in seen:
            seen.add(key)
            result.append(variant)

    return result


def _single_phrase_match(phrase: str, clean_text: str) -> bool:
    """
    phrase의 단어 순서/연속성을 유지해서 검색합니다.
    마지막 단어는 자연스러운 단수/복수형을 자동 허용합니다.
    """
    for variant in phrase_variants(phrase):
        parts = variant.split(" ")
        pattern_body = r"\s+".join(
            re.escape(part) for part in parts
        )
        pattern = rf"(?<!\w){pattern_body}(?!\w)"

        if re.search(
            pattern,
            clean_text,
            flags=re.IGNORECASE,
        ):
            return True

    return False


def exact_phrase_match(keyword: str, text: str) -> bool:
    """
    실제 기사 후처리 검색 규칙.

    1) AND 없는 경우:
       전체 입력값을 하나의 연속 phrase로 검색.
       마지막 단어의 자연스러운 단수/복수는 자동 허용.

       AI Policy
       -> AI Policy / AI Policies
       -> AI ... Policy 는 불일치

    2) AND 있는 경우:
       AND로 나눈 각 phrase가 기사 안에 모두 존재해야 함.
       phrase끼리는 떨어져 있어도 되고 순서는 상관없음.

       "AI" AND "Data Center"
       -> AI + Data Center  : 일치
       -> AI + Data Centers : 일치
       -> Data Centers + AI : 일치
       -> AI만 존재         : 불일치
    """
    expressions = parse_keyword_expression(keyword)
    if not expressions:
        return False

    clean_text = BeautifulSoup(
        text or "",
        "html.parser",
    ).get_text(" ", strip=True)
    clean_text = re.sub(r"\s+", " ", clean_text)

    return all(
        _single_phrase_match(expression, clean_text)
        for expression in expressions
    )


def _google_phrase_query(phrase: str) -> str:
    """
    Google News에 보낼 phrase 쿼리.
    단수/복수형을 OR로 넓혀 검색하고,
    최종 정확성은 exact_phrase_match에서 다시 검증합니다.
    """
    variants = phrase_variants(phrase)

    if not variants:
        return ""

    quoted = [f'"{variant}"' for variant in variants]

    if len(quoted) == 1:
        return quoted[0]

    return "(" + " OR ".join(quoted) + ")"


def google_keyword_query(keyword: str) -> str:
    """
    사용자 키워드를 Google News 검색 문법으로 변환.

    AI Policy
    -> ("AI Policy" OR "AI Policies")

    "AI" AND "Data Center"
    -> "AI" AND ("Data Center" OR "Data Centers")
    """
    expressions = parse_keyword_expression(keyword)

    if not expressions:
        return ""

    phrase_queries = [
        _google_phrase_query(expression)
        for expression in expressions
    ]
    phrase_queries = [q for q in phrase_queries if q]

    if not phrase_queries:
        return ""

    if len(phrase_queries) == 1:
        return phrase_queries[0]

    return "(" + " AND ".join(phrase_queries) + ")"


# =========================================================
# 3. 설정값 읽기 / 저장
# =========================================================

def get_settings():
    with SessionLocal() as session:
        categories = session.scalars(
            select(Category).order_by(Category.sort_order)
        ).all()

        keywords = {}
        for category in categories:
            keywords[category.name] = session.scalars(
                select(Keyword.keyword)
                .where(Keyword.category_id == category.id)
                .order_by(Keyword.id)
            ).all()

        domains = session.scalars(
            select(Source.domain)
            .where(Source.enabled.is_(True))
            .order_by(Source.id)
        ).all()

    return keywords, list(domains)


def save_settings(keyword_map: dict[str, list[str]], domains: list[str]):
    clean_domains = []
    seen_domains = set()

    for raw in domains:
        domain = normalize_domain(raw)
        if domain and domain not in seen_domains:
            clean_domains.append(domain)
            seen_domains.add(domain)

    if not clean_domains:
        raise ValueError("언론사 사이트를 최소 1개 이상 입력해주세요.")

    with SessionLocal() as session:
        categories = session.scalars(
            select(Category).order_by(Category.sort_order)
        ).all()
        category_by_name = {c.name: c for c in categories}

        session.execute(delete(Keyword))
        session.execute(delete(Source))

        for category_name in NEWS_CATEGORY_NAMES:
            category = category_by_name[category_name]
            seen_kw = set()

            for raw_kw in keyword_map.get(category_name, []):
                kw = normalize_keyword(raw_kw)
                if kw and kw.lower() not in seen_kw:
                    session.add(Keyword(category_id=category.id, keyword=kw))
                    seen_kw.add(kw.lower())

        for domain in clean_domains:
            session.add(Source(domain=domain, enabled=True))

        session.commit()


def _gemini_day_start_utc() -> datetime:
    """Gemini 일일 quota 리셋 기준(Pacific Time)의 오늘 00:00을 UTC로 반환."""
    now_pt = datetime.now(GEMINI_QUOTA_TZ)
    start_pt = now_pt.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_pt.astimezone(timezone.utc)


def summary_usage_today(mode: str) -> int:
    start_utc = _gemini_day_start_utc()
    with SessionLocal() as session:
        return session.scalar(
            select(func.count(SummaryUsage.id)).where(
                SummaryUsage.mode == mode,
                SummaryUsage.used_at >= start_utc,
            )
        ) or 0


def summary_limit(mode: str) -> int:
    return AUTO_SUMMARY_DAILY_LIMIT if mode == "auto" else MANUAL_SUMMARY_DAILY_LIMIT


def remaining_summary_quota(mode: str) -> int:
    return max(0, summary_limit(mode) - summary_usage_today(mode))


def record_summary_usage(mode: str, article_id: int | None):
    with SessionLocal() as session:
        session.add(
            SummaryUsage(
                article_id=article_id,
                mode=mode,
                used_at=datetime.now(timezone.utc),
            )
        )
        session.commit()


def get_summary_quota_status() -> dict:
    auto_used = summary_usage_today("auto")
    manual_used = summary_usage_today("manual")
    return {
        "auto_used": auto_used,
        "auto_limit": AUTO_SUMMARY_DAILY_LIMIT,
        "auto_remaining": max(0, AUTO_SUMMARY_DAILY_LIMIT - auto_used),
        "manual_used": manual_used,
        "manual_limit": MANUAL_SUMMARY_DAILY_LIMIT,
        "manual_remaining": max(0, MANUAL_SUMMARY_DAILY_LIMIT - manual_used),
    }


# =========================================================
# 4. Gemini 3줄 요약
# =========================================================

def summarize_article(title: str, description: str) -> str | None:
    global _LAST_SUMMARY_ERROR
    _LAST_SUMMARY_ERROR = None

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        _LAST_SUMMARY_ERROR = "missing_key"
        return None

    clean_description = BeautifulSoup(
        description or "", "html.parser"
    ).get_text(" ", strip=True)
    clean_description = clean_description[:2500]

    prompt = f"""
아래 뉴스 기사 또는 공개 소셜 게시물의 제목/원문을 바탕으로,
글로벌 대외협력(GPA) 담당자가 빠르게 핵심을 파악할 수 있도록 한국어로 요약해 주세요.

규칙:
- 정확히 3개의 짧은 불릿으로 작성
- 확인되지 않은 내용을 추측하지 말 것
- 회사명, 기관명, 정책명 등 핵심 고유명사는 가능하면 유지
- 각 불릿은 한 문장 정도로 간결하게 작성

제목:
{title}

원문/설명:
{clean_description}
""".strip()

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        text = (response.text or "").strip()
        return text or None
    except Exception as exc:
        error_text = str(exc)
        if "429" in error_text or "RESOURCE_EXHAUSTED" in error_text:
            _LAST_SUMMARY_ERROR = "quota"
        else:
            _LAST_SUMMARY_ERROR = "other"
        print(f"[Gemini summary error] {exc}")
        return None


def update_article_summary(article_id: int, summary: str | None):
    if not summary:
        return

    with SessionLocal() as session:
        article = session.get(Article, article_id)
        if article:
            article.summary = summary
            session.commit()



# =========================================================
# V3 Hybrid Collector 공통 도우미
# =========================================================

def _host_from_url(value: str) -> str:
    try:
        parsed = urllib.parse.urlparse(value or "")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _host_matches_domain(host: str, domain: str) -> bool:
    host = (host or "").lower().strip(".")
    domain = normalize_domain(domain).split("/")[0].lower().strip(".")
    return bool(
        host
        and domain
        and (host == domain or host.endswith("." + domain))
    )


def _source_key(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", value).strip()


def _clean_feed_title(title: str, source: str) -> str:
    """
    Google News가 제목 뒤에 붙이는 ' - Reuters' 같은 source suffix를 제거.
    직접 센싱 제목과 중복 제거가 더 잘 되게 합니다.
    """
    title = re.sub(r"\s+", " ", title or "").strip()
    source = re.sub(r"\s+", " ", source or "").strip()

    if not title or not source:
        return title

    for sep in (" - ", " | ", " — "):
        suffix = sep + source
        if title.lower().endswith(suffix.lower()):
            return title[:-len(suffix)].strip()

    return title


def _dedupe_title_key(title: str) -> str:
    title = BeautifulSoup(title or "", "html.parser").get_text(" ", strip=True)
    title = title.lower()
    title = re.sub(r"[^a-z0-9가-힣]+", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def _parse_flexible_datetime(value) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    raw = str(value).strip()
    if not raw:
        return None

    # RSS / HTTP 형식
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # ISO 8601
    try:
        cleaned = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # September 3, 2026 / Sep 3, 2026
    for fmt in (
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
    ):
        try:
            match = re.search(
                r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|"
                r"May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|"
                r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+"
                r"\d{1,2},?\s+20\d{2}\b",
                raw,
                flags=re.IGNORECASE,
            )
            if match:
                value = match.group(0)
                if "," not in fmt:
                    value = value.replace(",", "")
                dt = datetime.strptime(value, fmt)
                return dt.replace(
                    hour=12,
                    minute=0,
                    tzinfo=timezone.utc,
                )
        except Exception:
            pass

    # YYYY-MM-DD
    match = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", raw)
    if match:
        try:
            return datetime(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
                12,
                0,
                tzinfo=timezone.utc,
            )
        except Exception:
            pass

    return None


def _is_recent_enough(dt: datetime | None, hours: int) -> bool:
    if not dt:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return dt >= cutoff


def _canonicalize_url(url: str) -> str:
    """tracking query/fragment를 제거해 URL 중복을 줄입니다."""
    try:
        parsed = urllib.parse.urlparse(url or "")
        if not parsed.scheme or not parsed.netloc:
            return url or ""

        keep_query = []
        for key, value in urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
        ):
            if key.lower().startswith("utm_"):
                continue
            if key.lower() in {
                "gclid",
                "fbclid",
                "cmpid",
                "mod",
                "output",
            }:
                continue
            keep_query.append((key, value))

        return urllib.parse.urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urllib.parse.urlencode(keep_query),
                "",
            )
        )
    except Exception:
        return url or ""


def _is_likely_article_url(url: str, domain: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        host = _host_from_url(url)
        if not _host_matches_domain(host, domain):
            return False

        path = urllib.parse.unquote(parsed.path or "")
        path_lower = path.lower()

        if not path or path == "/":
            return False

        blocked_fragments = (
            "/author/",
            "/authors/",
            "/tag/",
            "/tags/",
            "/topic/",
            "/topics/",
            "/search",
            "/login",
            "/signin",
            "/subscribe",
            "/account",
            "/privacy",
            "/terms",
            "/contact",
            "/about",
            "/newsletters",
            "/podcasts",
            "/video/",
            "/videos/",
        )
        if any(fragment in path_lower for fragment in blocked_fragments):
            return False

        if re.search(
            r"\.(jpg|jpeg|png|gif|webp|svg|pdf|xml|rss|zip)$",
            path_lower,
        ):
            return False

        segments = [s for s in path.split("/") if s]
        if len(segments) < 2:
            return False

        # 날짜 URL, FT content UUID, 긴 기사 slug 등을 허용
        if re.search(r"/20\d{2}/", path):
            return True
        if "/content/" in path_lower:
            return True
        if "/news/articles/" in path_lower:
            return True
        if "/releases/" in path_lower:
            return True
        if "/briefings-statements/" in path_lower:
            return True
        if "/presidential-actions/" in path_lower:
            return True

        last = segments[-1]
        return len(last) >= 24

    except Exception:
        return False



def _url_allowed_for_profile(
    url: str,
    domain: str,
    profile: dict | None = None,
) -> bool:
    """
    공통 article URL 검사 + 출처별 허용 경로 검사.
    Government scope처럼 특정 섹션만 센싱할 때 사용합니다.
    """
    if not _is_likely_article_url(url, domain):
        return False

    if not profile:
        return True

    prefixes = profile.get("allowed_path_prefixes") or []
    if not prefixes:
        return True

    try:
        path = urllib.parse.urlparse(url).path or "/"
    except Exception:
        return False

    return any(
        path.startswith(prefix)
        for prefix in prefixes
    )

def _safe_get(url: str, timeout: int | None = None):
    return requests.get(
        url,
        timeout=timeout or DIRECT_TIMEOUT_SECONDS,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36 "
                "GPA-News-Sensing/3.0"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
        allow_redirects=True,
    )


def _extract_sitemaps_from_robots(content: str) -> list[str]:
    result = []
    for line in (content or "").splitlines():
        if line.lower().startswith("sitemap:"):
            value = line.split(":", 1)[1].strip()
            if value.startswith("http"):
                result.append(value)
    return result


def _xml_local_name(tag: str) -> str:
    return (tag or "").split("}")[-1].lower()


def _parse_sitemap_xml(content: bytes) -> dict:
    """
    반환:
    {
      "type": "index" | "urlset" | "unknown",
      "items": [{"loc": ..., "lastmod": datetime|None}, ...]
    }
    """
    try:
        root = ET.fromstring(content)
    except Exception:
        return {"type": "unknown", "items": []}

    root_type = _xml_local_name(root.tag)
    items = []

    for child in list(root):
        loc = None
        lastmod = None

        for node in list(child):
            name = _xml_local_name(node.tag)
            text = (node.text or "").strip()

            if name == "loc":
                loc = text
            elif name in {"lastmod", "publication_date"}:
                parsed = _parse_flexible_datetime(text)
                if parsed:
                    lastmod = parsed

        if loc:
            items.append(
                {
                    "loc": loc,
                    "lastmod": lastmod,
                }
            )

    if root_type == "sitemapindex":
        kind = "index"
    elif root_type == "urlset":
        kind = "urlset"
    else:
        kind = "unknown"

    return {"type": kind, "items": items}


def _sitemap_child_score(item: dict) -> tuple:
    loc = (item.get("loc") or "").lower()
    lastmod = item.get("lastmod")

    score = 0
    for token in (
        "news",
        "article",
        "post",
        "story",
        "release",
        "press",
        "2026",
    ):
        if token in loc:
            score += 3

    if lastmod:
        age = datetime.now(timezone.utc) - lastmod
        if age <= timedelta(days=2):
            score += 10
        elif age <= timedelta(days=7):
            score += 6
        elif age <= timedelta(days=31):
            score += 2

    return (score, lastmod or datetime.min.replace(tzinfo=timezone.utc))


def _extract_listing_links(
    html: str,
    base_url: str,
    domain: str,
) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    links = []

    # canonical/amp 링크는 candidate가 아니라 현재 listing 자신일 수 있어 제외
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        absolute = urllib.parse.urljoin(base_url, href)
        absolute = _canonicalize_url(absolute)

        if _is_likely_article_url(absolute, domain):
            links.append(absolute)

    # 순서 유지 중복 제거
    return list(dict.fromkeys(links))


def _discover_direct_candidates_for_source(
    domain: str,
    profile: dict,
) -> dict:
    """
    홈페이지/섹션 + robots.txt + sitemap을 이용해
    직접 기사 URL 후보를 모읍니다.
    """
    start_pages = profile.get("start_pages", [])

    if profile.get("listing_only"):
        # State Press Releases / White House News처럼
        # 특정 허브 페이지만 센싱하는 경우 사이트맵/robots 전체 탐색 금지
        probe_urls = list(start_pages)
    else:
        probe_urls = [
            f"https://{domain}/robots.txt",
            *start_pages,
            *[
                f"https://{domain}{path}"
                for path in COMMON_SITEMAP_PATHS
            ],
        ]

    candidates = []
    sitemap_urls = []
    successful_probes = 0

    # 1차 probe
    workers = max(1, min(8, len(probe_urls)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(_safe_get, url): url
            for url in probe_urls
        }

        for future in as_completed(future_map):
            url = future_map[future]

            try:
                response = future.result()
                if response.status_code >= 400:
                    continue

                successful_probes += 1
                ctype = (
                    response.headers.get("content-type", "")
                    .lower()
                )

                if url.endswith("/robots.txt"):
                    sitemap_urls.extend(
                        _extract_sitemaps_from_robots(
                            response.text
                        )
                    )
                    continue

                is_xml = (
                    "xml" in ctype
                    or url.endswith(".xml")
                    or response.content.lstrip().startswith(b"<?xml")
                )

                if is_xml:
                    parsed = _parse_sitemap_xml(response.content)
                    if parsed["type"] == "index":
                        ranked = sorted(
                            parsed["items"],
                            key=_sitemap_child_score,
                            reverse=True,
                        )
                        sitemap_urls.extend(
                            item["loc"]
                            for item in ranked[
                                :DIRECT_SITEMAPS_PER_SOURCE
                            ]
                        )
                    elif parsed["type"] == "urlset":
                        for item in parsed["items"]:
                            if not _is_recent_enough(
                                item["lastmod"],
                                DIRECT_LOOKBACK_HOURS,
                            ):
                                continue
                            if _url_allowed_for_profile(
                                item["loc"],
                                domain,
                                profile,
                            ):
                                candidates.append(
                                    _canonicalize_url(
                                        item["loc"]
                                    )
                                )
                    continue

                # 일반 HTML listing
                listing_links = _extract_listing_links(
                    response.text,
                    response.url or url,
                    domain,
                )
                candidates.extend(
                    candidate_url
                    for candidate_url in listing_links
                    if _url_allowed_for_profile(
                        candidate_url,
                        domain,
                        profile,
                    )
                )

            except Exception:
                continue

    # Sitemap URL 중복 제거 + 동일 도메인만
    sitemap_urls = [
        u for u in dict.fromkeys(sitemap_urls)
        if _host_matches_domain(_host_from_url(u), domain)
    ][:DIRECT_SITEMAPS_PER_SOURCE * 2]

    # 2차 sitemap probe
    if sitemap_urls:
        workers = max(
            1,
            min(
                DIRECT_MAX_WORKERS,
                len(sitemap_urls),
            ),
        )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(_safe_get, url): url
                for url in sitemap_urls
            }

            nested_sitemaps = []

            for future in as_completed(future_map):
                try:
                    response = future.result()
                    if response.status_code >= 400:
                        continue

                    successful_probes += 1
                    parsed = _parse_sitemap_xml(
                        response.content
                    )

                    if parsed["type"] == "urlset":
                        recent_items = sorted(
                            parsed["items"],
                            key=lambda item: (
                                item["lastmod"]
                                or datetime.min.replace(
                                    tzinfo=timezone.utc
                                )
                            ),
                            reverse=True,
                        )

                        for item in recent_items[:150]:
                            if not _is_recent_enough(
                                item["lastmod"],
                                DIRECT_LOOKBACK_HOURS,
                            ):
                                continue

                            url = _canonicalize_url(
                                item["loc"]
                            )
                            if _url_allowed_for_profile(
                                url,
                                domain,
                                profile,
                            ):
                                candidates.append(url)

                    elif parsed["type"] == "index":
                        ranked = sorted(
                            parsed["items"],
                            key=_sitemap_child_score,
                            reverse=True,
                        )
                        nested_sitemaps.extend(
                            item["loc"]
                            for item in ranked[
                                :DIRECT_SITEMAPS_PER_SOURCE
                            ]
                        )

                except Exception:
                    continue

            # sitemap index -> sitemap 한 단계 더
            nested_sitemaps = [
                u for u in dict.fromkeys(nested_sitemaps)
                if _host_matches_domain(
                    _host_from_url(u),
                    domain,
                )
            ][:DIRECT_SITEMAPS_PER_SOURCE]

            if nested_sitemaps:
                with ThreadPoolExecutor(
                    max_workers=min(
                        DIRECT_MAX_WORKERS,
                        len(nested_sitemaps),
                    )
                ) as executor:
                    future_map = {
                        executor.submit(
                            _safe_get,
                            url,
                        ): url
                        for url in nested_sitemaps
                    }

                    for future in as_completed(future_map):
                        try:
                            response = future.result()
                            if response.status_code >= 400:
                                continue

                            successful_probes += 1
                            parsed = _parse_sitemap_xml(
                                response.content
                            )

                            if parsed["type"] != "urlset":
                                continue

                            recent_items = sorted(
                                parsed["items"],
                                key=lambda item: (
                                    item["lastmod"]
                                    or datetime.min.replace(
                                        tzinfo=timezone.utc
                                    )
                                ),
                                reverse=True,
                            )

                            for item in recent_items[:150]:
                                if not _is_recent_enough(
                                    item["lastmod"],
                                    DIRECT_LOOKBACK_HOURS,
                                ):
                                    continue

                                url = _canonicalize_url(
                                    item["loc"]
                                )
                                if _url_allowed_for_profile(
                                    url,
                                    domain,
                                    profile,
                                ):
                                    candidates.append(url)

                        except Exception:
                            continue

    # URL 순서 유지 중복 제거.
    # start page에서 나온 최신 링크가 앞쪽에 있으므로 먼저 보존.
    candidates = list(dict.fromkeys(candidates))

    return {
        "domain": domain,
        "candidates": candidates[
            :DIRECT_MAX_ARTICLES_PER_SOURCE
        ],
        "successful_probes": successful_probes,
    }


def _extract_jsonld_objects(soup: BeautifulSoup) -> list:
    objects = []

    for script in soup.find_all(
        "script",
        attrs={"type": "application/ld+json"},
    ):
        raw = script.string or script.get_text()
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        queue = [data]
        while queue:
            current = queue.pop()

            if isinstance(current, dict):
                objects.append(current)

                graph = current.get("@graph")
                if isinstance(graph, list):
                    queue.extend(graph)

            elif isinstance(current, list):
                queue.extend(current)

    return objects


def _meta_content(
    soup: BeautifulSoup,
    *,
    property_name: str | None = None,
    name: str | None = None,
) -> str:
    attrs = {}

    if property_name:
        attrs["property"] = property_name
    elif name:
        attrs["name"] = name
    else:
        return ""

    tag = soup.find("meta", attrs=attrs)
    if not tag:
        return ""

    return str(tag.get("content") or "").strip()


def _extract_direct_article(url: str, domain: str, label: str) -> dict:
    """
    공개 웹페이지에서 제목/날짜/본문(가능한 범위)을 직접 추출합니다.
    paywall/봇차단으로 읽지 못하면 error를 반환하고 Google News 보조망에 맡깁니다.
    """
    try:
        response = _safe_get(url)
        if response.status_code >= 400:
            return {
                "url": url,
                "error": f"HTTP {response.status_code}",
            }

        final_url = _canonicalize_url(
            response.url or url
        )

        if not _host_matches_domain(
            _host_from_url(final_url),
            domain,
        ):
            return {
                "url": url,
                "error": "redirected outside target domain",
            }

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        canonical = soup.find(
            "link",
            attrs={"rel": "canonical"},
        )
        if canonical and canonical.get("href"):
            candidate_canonical = _canonicalize_url(
                urllib.parse.urljoin(
                    final_url,
                    canonical.get("href"),
                )
            )
            if _host_matches_domain(
                _host_from_url(candidate_canonical),
                domain,
            ):
                final_url = candidate_canonical

        title = (
            _meta_content(
                soup,
                property_name="og:title",
            )
            or _meta_content(
                soup,
                name="twitter:title",
            )
            or (
                soup.title.get_text(" ", strip=True)
                if soup.title
                else ""
            )
        )
        title = re.sub(r"\s+", " ", title).strip()

        description = (
            _meta_content(
                soup,
                property_name="og:description",
            )
            or _meta_content(
                soup,
                name="description",
            )
            or _meta_content(
                soup,
                name="twitter:description",
            )
        )

        published_at = None
        article_body = ""

        jsonld_objects = _extract_jsonld_objects(soup)

        for obj in jsonld_objects:
            obj_type = obj.get("@type")
            if isinstance(obj_type, list):
                types = {
                    str(v).lower()
                    for v in obj_type
                }
            else:
                types = {str(obj_type or "").lower()}

            if types.intersection(
                {
                    "newsarticle",
                    "article",
                    "reportagenewsarticle",
                    "analysisnewsarticle",
                }
            ):
                if not title:
                    title = str(
                        obj.get("headline") or ""
                    ).strip()

                if not published_at:
                    published_at = (
                        _parse_flexible_datetime(
                            obj.get("datePublished")
                        )
                        or _parse_flexible_datetime(
                            obj.get("dateCreated")
                        )
                    )

                if not description:
                    description = str(
                        obj.get("description") or ""
                    ).strip()

                if not article_body:
                    article_body = str(
                        obj.get("articleBody") or ""
                    ).strip()

        if not published_at:
            for key_type, key in (
                ("property", "article:published_time"),
                ("property", "og:published_time"),
                ("name", "date"),
                ("name", "parsely-pub-date"),
                ("name", "sailthru.date"),
            ):
                if key_type == "property":
                    raw = _meta_content(
                        soup,
                        property_name=key,
                    )
                else:
                    raw = _meta_content(
                        soup,
                        name=key,
                    )

                published_at = _parse_flexible_datetime(
                    raw
                )
                if published_at:
                    break

        if not published_at:
            time_tag = soup.find(
                "time",
                attrs={"datetime": True},
            )
            if time_tag:
                published_at = _parse_flexible_datetime(
                    time_tag.get("datetime")
                )

        # JSON-LD articleBody가 없으면 공개된 article/main 문단에서 텍스트 추출
        if not article_body:
            container = (
                soup.find("article")
                or soup.find("main")
                or soup
            )
            paragraphs = []

            for p in container.find_all("p"):
                text = re.sub(
                    r"\s+",
                    " ",
                    p.get_text(" ", strip=True),
                ).strip()

                if len(text) >= 30:
                    paragraphs.append(text)

                if sum(
                    len(v) for v in paragraphs
                ) >= 14000:
                    break

            article_body = "\n".join(paragraphs)

        combined = "\n".join(
            value
            for value in (
                title,
                description,
                article_body,
            )
            if value
        )
        combined = combined[:16000]

        if not title:
            return {
                "url": final_url,
                "error": "no title",
            }

        # direct 후보라도 실제 발행일이 너무 오래됐으면 제외
        if (
            published_at
            and not _is_recent_enough(
                published_at,
                DIRECT_LOOKBACK_HOURS,
            )
        ):
            return {
                "url": final_url,
                "skip": "old",
            }

        return {
            "url": final_url,
            "title": title,
            "description": (
                description
                or article_body[:2500]
            ),
            "search_text": combined,
            "published_at": published_at,
            "source": label,
            "error": None,
        }

    except Exception as exc:
        return {
            "url": url,
            "error": str(exc),
        }



def _extract_native_search_candidates(
    html: str,
    search_url: str,
    target_domain: str,
    allowed_path_prefixes: list[str] | None = None,
) -> list[dict]:
    """
    사이트 자체 검색결과에서 실제 기사 링크 + 제목 + 주변 문맥/날짜를 추출.
    State.gov는 검색페이지가 findit.state.gov에 있어도 최종 링크가
    state.gov이면 후보로 인정합니다.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    candidates = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        absolute = _canonicalize_url(
            urllib.parse.urljoin(search_url, href)
        )

        if not _host_matches_domain(
            _host_from_url(absolute),
            target_domain,
        ):
            continue

        if not _is_likely_article_url(
            absolute,
            target_domain,
        ):
            continue

        if allowed_path_prefixes:
            path = urllib.parse.urlparse(
                absolute
            ).path or "/"
            if not any(
                path.startswith(prefix)
                for prefix in allowed_path_prefixes
            ):
                continue

        if absolute in seen:
            continue
        seen.add(absolute)

        title = re.sub(
            r"\s+",
            " ",
            a.get_text(" ", strip=True),
        ).strip()

        parent = (
            a.find_parent("article")
            or a.find_parent("li")
            or a.find_parent("div")
            or a.parent
        )
        context = ""
        if parent:
            context = re.sub(
                r"\s+",
                " ",
                parent.get_text(" ", strip=True),
            ).strip()

        if not title and context:
            title = context[:220]

        candidates.append(
            {
                "url": absolute,
                "title": title,
                "context": context[:1800],
                "published_at": _parse_flexible_datetime(
                    context
                ),
            }
        )

        if len(candidates) >= NATIVE_SEARCH_RESULTS_PER_KEYWORD:
            break

    return candidates


def collect_native_keyword_searches(
    keyword_map: dict[str, list[str]],
    enabled_domains: list[str],
) -> dict:
    """
    V3.2 핵심 보완:
    등록된 키워드를 각 사이트 자체 검색페이지에 직접 넣습니다.

    장점:
    - Reuters의 특정 기사처럼 최신 listing 첫 화면에서 빠진 기사도 검색 가능
    - White House는 자체 검색으로 Releases/News를 직접 찾음
    - State.gov 원문 403이어도 Search.gov 결과에서 링크/제목을 발견하면
      fallback으로 DB에 저장 가능
    """
    enabled = []
    seen_domains = set()

    for raw in enabled_domains:
        domain = normalize_domain(raw).split("/")[0]
        if (
            domain in NATIVE_SEARCH_TEMPLATES
            and domain not in seen_domains
        ):
            enabled.append(domain)
            seen_domains.add(domain)

    stats = {
        "search_pages_checked": 0,
        "search_page_failures": 0,
        "candidate_urls": 0,
        "article_pages_checked": 0,
        "article_page_failures": 0,
        "fallback_saved": 0,
        "matched_articles": 0,
        "new_articles": 0,
    }

    if not enabled:
        return stats

    search_jobs = []

    for category_name in NEWS_CATEGORY_NAMES:
        for keyword in keyword_map.get(category_name, []):
            for domain in enabled:
                profile = NATIVE_SEARCH_TEMPLATES[domain]

                search_term = keyword
                if domain == "state.gov":
                    # Search.gov가 State 전체 페이지를 반환하지 않도록
                    # 실제 press release URL 경로로 검색 범위 제한
                    search_term = (
                        f'{keyword} site:state.gov/releases/'
                    )

                query = urllib.parse.quote_plus(
                    search_term
                )
                search_url = profile["url"].format(
                    query=query
                )

                scope_profile = DIRECT_SOURCE_PROFILES.get(
                    domain,
                    {},
                )

                search_jobs.append(
                    {
                        "category": category_name,
                        "keyword": keyword,
                        "domain": domain,
                        "label": profile["label"],
                        "url": search_url,
                        "allowed_path_prefixes": (
                            scope_profile.get(
                                "allowed_path_prefixes"
                            )
                            or []
                        ),
                    }
                )

    candidate_map = {}

    workers = max(
        1,
        min(DIRECT_MAX_WORKERS, len(search_jobs)),
    )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(_safe_get, job["url"]): job
            for job in search_jobs
        }

        for future in as_completed(future_map):
            job = future_map[future]

            try:
                response = future.result()
                if response.status_code >= 400:
                    stats["search_page_failures"] += 1
                    continue

                stats["search_pages_checked"] += 1

                candidates = _extract_native_search_candidates(
                    response.text,
                    response.url or job["url"],
                    job["domain"],
                    job.get("allowed_path_prefixes"),
                )

                for candidate in candidates:
                    # 검색결과 자체에도 키워드가 있어야 후보 유지
                    search_text = (
                        f"{candidate['title']} "
                        f"{candidate['context']}"
                    )
                    if not exact_phrase_match(
                        job["keyword"],
                        search_text,
                    ):
                        continue

                    url = candidate["url"]
                    item = candidate_map.setdefault(
                        url,
                        {
                            "url": url,
                            "domain": job["domain"],
                            "label": job["label"],
                            "published_at": candidate[
                                "published_at"
                            ],
                            "title": candidate["title"],
                            "context": candidate["context"],
                            "matches": [],
                        },
                    )

                    item["matches"].append(
                        (
                            job["category"],
                            job["keyword"],
                        )
                    )

                    if (
                        not item["published_at"]
                        and candidate["published_at"]
                    ):
                        item["published_at"] = candidate[
                            "published_at"
                        ]

                    if (
                        len(candidate["context"])
                        > len(item["context"])
                    ):
                        item["context"] = candidate[
                            "context"
                        ]

            except Exception as exc:
                print(
                    f"[Native search error] "
                    f"{job['domain']}: {exc}"
                )
                stats["search_page_failures"] += 1

    candidates = list(candidate_map.values())[
        :NATIVE_SEARCH_MAX_ARTICLE_FETCHES
    ]
    stats["candidate_urls"] = len(candidates)

    if not candidates:
        return stats

    new_ids = set()

    with ThreadPoolExecutor(
        max_workers=min(
            DIRECT_MAX_WORKERS,
            len(candidates),
        )
    ) as executor:
        future_map = {
            executor.submit(
                _extract_direct_article,
                item["url"],
                item["domain"],
                item["label"],
            ): item
            for item in candidates
        }

        for future in as_completed(future_map):
            item = future_map[future]

            try:
                article_data = future.result()
            except Exception as exc:
                article_data = {
                    "error": str(exc)
                }

            page_ok = not (
                article_data.get("error")
                or article_data.get("skip")
            )

            if page_ok:
                stats["article_pages_checked"] += 1
                final_title = article_data["title"]
                final_url = article_data["url"]
                final_description = article_data[
                    "description"
                ]
                final_published = (
                    article_data["published_at"]
                    or item["published_at"]
                )
                search_text = article_data[
                    "search_text"
                ]
            else:
                # State.gov 등 자동접근 403 시 검색결과 정보로 fallback
                stats["article_page_failures"] += 1
                final_title = (
                    item["title"]
                    or "(제목 정보 없음)"
                )
                final_url = item["url"]
                final_description = item["context"]
                final_published = item["published_at"]

                # 발행시간을 못 얻어도 DB에서 사라지지 않도록
                # detected_at 기준 fallback을 화면 필터에서 사용합니다.
                search_text = (
                    f"{final_title} "
                    f"{final_description}"
                )

            for category_name, keyword in item["matches"]:
                if not exact_phrase_match(
                    keyword,
                    search_text,
                ):
                    continue

                stats["matched_articles"] += 1

                try:
                    article_id, is_new, _ = upsert_article(
                        category_name=category_name,
                        title=final_title,
                        link=final_url,
                        source=item["label"],
                        published_at=final_published,
                        description=final_description,
                        matched_keywords=[keyword],
                    )

                    if not page_ok:
                        stats["fallback_saved"] += 1

                    if (
                        is_new
                        and article_id
                        and article_id not in new_ids
                    ):
                        new_ids.add(article_id)
                        stats["new_articles"] += 1

                except Exception as exc:
                    print(
                        f"[Native search save error] "
                        f"{item['domain']}: {exc}"
                    )

    return stats

def collect_direct_sources(
    keyword_map: dict[str, list[str]],
    enabled_domains: list[str],
) -> dict:
    """
    9개 핵심 출처를 직접 확인해 Google News 누락을 보완합니다.
    직접 접근이 막힌 매체는 실패해도 전체 수집은 계속되고,
    기존 Google News RSS가 보조망 역할을 합니다.
    """
    profiles = {}

    for configured in enabled_domains:
        base_domain = normalize_domain(
            configured
        ).split("/")[0]

        profile = DIRECT_SOURCE_PROFILES.get(
            base_domain
        )
        if profile:
            profiles[base_domain] = profile

    stats = {
        "sources_enabled": len(profiles),
        "sources_checked": 0,
        "candidate_urls": 0,
        "pages_checked": 0,
        "matched_articles": 0,
        "new_articles": 0,
        "page_failures": 0,
        "source_failures": 0,
    }

    if not profiles:
        return stats

    discovery_results = []

    with ThreadPoolExecutor(
        max_workers=min(
            DIRECT_MAX_WORKERS,
            len(profiles),
        )
    ) as executor:
        futures = {
            executor.submit(
                _discover_direct_candidates_for_source,
                domain,
                profile,
            ): domain
            for domain, profile in profiles.items()
        }

        for future in as_completed(futures):
            domain = futures[future]

            try:
                result = future.result()
                discovery_results.append(result)

                if result["successful_probes"] > 0:
                    stats["sources_checked"] += 1
                else:
                    stats["source_failures"] += 1

            except Exception as exc:
                print(
                    f"[Direct discovery error] "
                    f"{domain}: {exc}"
                )
                stats["source_failures"] += 1

    page_jobs = []

    for result in discovery_results:
        domain = result["domain"]
        profile = profiles[domain]

        for url in result["candidates"]:
            page_jobs.append(
                (
                    url,
                    domain,
                    profile["label"],
                )
            )

    # URL 기준 중복 제거
    unique_jobs = {}
    for url, domain, label in page_jobs:
        unique_jobs[url] = (
            url,
            domain,
            label,
        )

    page_jobs = list(unique_jobs.values())
    stats["candidate_urls"] = len(page_jobs)

    if not page_jobs:
        return stats

    new_article_ids = set()

    with ThreadPoolExecutor(
        max_workers=min(
            DIRECT_MAX_WORKERS,
            len(page_jobs),
        )
    ) as executor:
        futures = {
            executor.submit(
                _extract_direct_article,
                url,
                domain,
                label,
            ): (url, domain)
            for url, domain, label in page_jobs
        }

        for future in as_completed(futures):
            url, domain = futures[future]

            try:
                article_data = future.result()
            except Exception as exc:
                stats["page_failures"] += 1
                print(
                    f"[Direct page error] "
                    f"{domain}: {exc}"
                )
                continue

            if article_data.get("skip"):
                continue

            if article_data.get("error"):
                stats["page_failures"] += 1
                continue

            stats["pages_checked"] += 1
            search_text = article_data[
                "search_text"
            ]

            for category_name in NEWS_CATEGORY_NAMES:
                category_keywords = keyword_map.get(
                    category_name,
                    [],
                )
                if not category_keywords:
                    continue

                matched_keywords = [
                    kw
                    for kw in category_keywords
                    if exact_phrase_match(
                        kw,
                        search_text,
                    )
                ]

                if not matched_keywords:
                    continue

                stats["matched_articles"] += 1

                try:
                    article_id, is_new, _ = (
                        upsert_article(
                            category_name=category_name,
                            title=article_data["title"],
                            link=article_data["url"],
                            source=article_data["source"],
                            published_at=article_data[
                                "published_at"
                            ],
                            description=article_data[
                                "description"
                            ],
                            matched_keywords=matched_keywords,
                        )
                    )

                    if (
                        article_id
                        and is_new
                        and article_id
                        not in new_article_ids
                    ):
                        new_article_ids.add(
                            article_id
                        )
                        stats["new_articles"] += 1

                except Exception as exc:
                    print(
                        f"[Direct save error] "
                        f"{domain}: {exc}"
                    )

    return stats


def _feed_source_href(entry) -> str:
    try:
        source = entry.get("source")
        if not source:
            return ""

        return str(
            source.get("href")
            or source.get("url")
            or ""
        ).strip()
    except Exception:
        return ""


def _google_entry_matches_requested_source(
    entry,
    requested_domain: str,
) -> bool:
    """
    Google News가 site:reuters.com 검색 중 Daily Signal 같은
    다른 매체를 섞어 반환하는 것을 최종 차단합니다.
    """
    base_domain = normalize_domain(
        requested_domain
    ).split("/")[0]

    source_href = _feed_source_href(entry)
    if source_href:
        return _host_matches_domain(
            _host_from_url(source_href),
            base_domain,
        )

    # source URL이 없을 때는 알려진 9개 매체에 한해 이름 검증
    aliases = GOOGLE_SOURCE_ALIASES.get(
        base_domain
    )
    if aliases:
        source_title = _source_key(
            get_feed_source(entry)
        )
        return source_title in {
            _source_key(alias)
            for alias in aliases
        }

    # 기타 사용자가 직접 추가한 사이트는 기존 동작 유지
    return True


# =========================================================
# 5. Google News RSS 수집
# =========================================================

def parse_published(raw_value: str) -> datetime | None:
    try:
        dt = email.utils.parsedate_to_datetime(raw_value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def get_feed_source(entry) -> str:
    try:
        source = entry.get("source")
        if source and source.get("title"):
            return str(source.get("title"))
    except Exception:
        pass
    return "Unknown"


def article_hash(link: str) -> str:
    return hashlib.sha256((link or "").encode("utf-8")).hexdigest()


def upsert_article(
    category_name: str,
    title: str,
    link: str,
    source: str,
    published_at: datetime | None,
    description: str,
    matched_keywords: list[str],
) -> tuple[int | None, bool, bool]:
    """
    반환:
    article_id, 새 기사인지, 요약이 필요한지
    """
    if not link:
        return None, False, False

    url_hash = article_hash(link)

    with SessionLocal() as session:
        category = session.scalar(
            select(Category).where(Category.name == category_name)
        )
        if not category:
            return None, False, False

        article = session.scalar(
            select(Article).where(Article.url_hash == url_hash)
        )

        # Google News redirect URL과 직접 원문 URL이 달라도
        # 같은 기사 제목이면 최근 DB 기사와 합쳐 중복을 줄입니다.
        if article is None and title:
            title_key = _dedupe_title_key(title)
            recent_cutoff = (
                datetime.now(timezone.utc)
                - timedelta(days=7)
            )

            recent_articles = session.scalars(
                select(Article)
                .where(
                    Article.detected_at >= recent_cutoff
                )
                .order_by(
                    Article.detected_at.desc()
                )
                .limit(1200)
            ).all()

            for candidate in recent_articles:
                if (
                    _dedupe_title_key(
                        candidate.title
                    )
                    == title_key
                ):
                    article = candidate
                    break

        is_new = False

        if article is None:
            article = Article(
                url_hash=url_hash,
                title=title or "(제목 없음)",
                link=link,
                source=source or "Unknown",
                published_at=published_at,
                detected_at=datetime.now(timezone.utc),
                description=description or "",
                summary=None,
            )
            session.add(article)
            session.flush()
            is_new = True
        else:
            # 기존 기사라도 더 좋은 정보가 있으면 보완
            if not article.published_at and published_at:
                article.published_at = published_at

            if description and (
                not article.description
                or len(description)
                > len(article.description)
            ):
                article.description = description

            if source and (
                not article.source
                or article.source == "Unknown"
            ):
                article.source = source

            # Google News redirect 대신 직접 원문 URL이 들어오면 원문으로 교체
            old_host = _host_from_url(article.link)
            new_host = _host_from_url(link)

            if (
                old_host == "news.google.com"
                and new_host
                and new_host != "news.google.com"
            ):
                new_hash = article_hash(link)
                hash_conflict = session.scalar(
                    select(Article.id).where(
                        Article.url_hash == new_hash,
                        Article.id != article.id,
                    )
                )

                if not hash_conflict:
                    article.link = link
                    article.url_hash = new_hash

        for kw in matched_keywords:
            exists = session.scalar(
                select(ArticleMatch.id).where(
                    ArticleMatch.article_id == article.id,
                    ArticleMatch.category_id == category.id,
                    ArticleMatch.keyword == kw,
                )
            )

            if not exists:
                session.add(
                    ArticleMatch(
                        article_id=article.id,
                        category_id=category.id,
                        keyword=kw,
                    )
                )

        needs_summary = not bool(article.summary)
        article_id = article.id
        session.commit()

    return article_id, is_new, needs_summary


def _fetch_feed_job(job: dict) -> dict:
    """Google News RSS 요청 1건을 timeout과 함께 가져옵니다."""
    try:
        response = requests.get(
            job["rss_url"],
            timeout=RSS_TIMEOUT_SECONDS,
            headers={"User-Agent": "Mozilla/5.0 GPA-News-Sensing/2.0"},
        )
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        return {**job, "entries": list(getattr(feed, "entries", [])), "error": None}
    except Exception as exc:
        return {**job, "entries": [], "error": str(exc)}


def collect_all_categories(generate_summaries: bool = True) -> dict:
    """
    V3.2 Keyword-First Hybrid

    1) 기존 latest/sitemap 직접 센싱
    2) 각 사이트 자체 검색페이지에 키워드 직접 검색
    3) Google News도 키워드 1개씩 검색
       (기존 4개 OR 묶음 제거 → 희귀 키워드가 묻히는 문제 완화)
    """
    keyword_map, domains = get_settings()

    stats = {
        "feeds_checked": 0,
        "matched_articles": 0,
        "new_articles": 0,
        "google_new_articles": 0,
        "summaries_created": 0,
        "errors": 0,
        "direct": {},
        "native": {},
    }

    # 1. 최신 페이지 / sitemap 기반 직접 센싱
    direct_stats = collect_direct_sources(
        keyword_map=keyword_map,
        enabled_domains=domains,
    )
    stats["direct"] = direct_stats
    stats["new_articles"] += direct_stats["new_articles"]

    # 2. 사이트 자체 검색페이지 기반 키워드 센싱
    native_stats = collect_native_keyword_searches(
        keyword_map=keyword_map,
        enabled_domains=domains,
    )
    stats["native"] = native_stats
    stats["new_articles"] += native_stats["new_articles"]

    # 3. Google News: 키워드 1개씩 + 여러 도메인을 묶어서 검색
    # 키워드 OR 묶음을 없애서 Foundry School 같은 희귀 키워드가
    # OpenAI/Texas 같은 넓은 키워드 결과에 밀리지 않게 합니다.
    clean_domains = []
    seen_domains = set()

    for raw in domains:
        value = normalize_domain(raw)
        if not value:
            continue

        base_domain = value.split("/")[0]

        # V3.2.2:
        # State/White House는 특정 공식 섹션만 허용하므로
        # 사이트 전체를 뒤지는 Google News 검색에서는 제외.
        if base_domain in {
            "state.gov",
            "whitehouse.gov",
        }:
            continue

        if value not in seen_domains:
            seen_domains.add(value)
            clean_domains.append(value)

    domain_chunk_size = 6
    domain_chunks = [
        clean_domains[i:i + domain_chunk_size]
        for i in range(
            0,
            len(clean_domains),
            domain_chunk_size,
        )
    ]

    jobs = []

    for category_name in NEWS_CATEGORY_NAMES:
        for keyword in keyword_map.get(category_name, []):
            kw_query = google_keyword_query(keyword)
            if not kw_query:
                continue

            for domain_chunk in domain_chunks:
                site_query = " OR ".join(
                    f"site:{domain}"
                    for domain in domain_chunk
                )

                full_query = (
                    f"({kw_query}) "
                    f"({site_query}) "
                    f"when:{SEARCH_LOOKBACK_HOURS}h"
                )

                encoded_query = urllib.parse.quote(
                    full_query
                )
                rss_url = (
                    "https://news.google.com/rss/search"
                    f"?q={encoded_query}"
                    "&hl=en-US&gl=US&ceid=US:en"
                )

                jobs.append(
                    {
                        "category_name": category_name,
                        "keyword": keyword,
                        "domain": ",".join(domain_chunk),
                        "rss_url": rss_url,
                    }
                )

    feed_results = []

    if jobs:
        workers = max(
            1,
            min(RSS_MAX_WORKERS, len(jobs)),
        )

        with ThreadPoolExecutor(
            max_workers=workers
        ) as executor:
            futures = [
                executor.submit(
                    _fetch_feed_job,
                    job,
                )
                for job in jobs
            ]

            for future in as_completed(futures):
                result = future.result()
                feed_results.append(result)
                stats["feeds_checked"] += 1

                if result["error"]:
                    print(
                        f"[Feed error] "
                        f"{result['domain']}: "
                        f"{result['error']}"
                    )
                    stats["errors"] += 1

    seen_links_by_category = {
        category_name: set()
        for category_name in NEWS_CATEGORY_NAMES
    }
    summary_queue = {}

    for result in feed_results:
        category_name = result["category_name"]
        keyword = result["keyword"]

        for entry in result["entries"]:
            source = get_feed_source(entry)
            raw_title = str(
                entry.get("title", "") or ""
            )
            title = _clean_feed_title(
                raw_title,
                source,
            )
            description_raw = str(
                entry.get("description", "") or ""
            )
            link = str(
                entry.get("link", "") or ""
            )

            if not link:
                continue

            # 같은 카테고리에서 동일 URL은 한 번만 처리
            if link in seen_links_by_category[
                category_name
            ]:
                continue

            text_to_search = (
                f"{title} {description_raw}"
            )

            if not exact_phrase_match(
                keyword,
                text_to_search,
            ):
                continue

            seen_links_by_category[
                category_name
            ].add(link)

            stats["matched_articles"] += 1

            published_at = parse_published(
                str(
                    entry.get("published", "")
                    or ""
                )
            )
            clean_description = BeautifulSoup(
                description_raw,
                "html.parser",
            ).get_text(" ", strip=True)

            try:
                article_id, is_new, needs_summary = (
                    upsert_article(
                        category_name=category_name,
                        title=title,
                        link=link,
                        source=source,
                        published_at=published_at,
                        description=clean_description,
                        matched_keywords=[keyword],
                    )
                )

                if is_new:
                    stats["new_articles"] += 1
                    stats["google_new_articles"] += 1

                if (
                    article_id
                    and needs_summary
                    and generate_summaries
                ):
                    summary_queue[article_id] = (
                        title,
                        clean_description,
                    )

            except Exception as exc:
                print(
                    f"[Article save error] {exc}"
                )
                stats["errors"] += 1

    if generate_summaries:
        for article_id, (
            title,
            clean_description,
        ) in summary_queue.items():
            summary = summarize_article(
                title=title,
                description=clean_description,
            )
            if summary:
                update_article_summary(
                    article_id,
                    summary,
                )
                stats["summaries_created"] += 1

    return stats


def _fetch_x_profile_timeline(handle: str) -> list[dict]:
    """
    X의 공개 임베드 타임라인(syndication)에서 공개 프로필 게시물을 가져옵니다.
    API Key가 필요 없는 무료 경로지만 X가 내부 구조를 바꾸면 동작이 깨질 수 있습니다.
    """
    url = (
        "https://syndication.twitter.com/srv/"
        f"timeline-profile/screen-name/{handle}"
    )

    response = requests.get(
        url,
        timeout=RSS_TIMEOUT_SECONDS,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124 Safari/537.36"
            )
        },
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script:
        raise ValueError("X 공개 타임라인 데이터(__NEXT_DATA__)를 찾지 못했습니다.")

    raw_json = script.string or script.get_text()
    data = json.loads(raw_json)

    page_props = data.get("props", {}).get("pageProps", {})
    entries = page_props.get("timeline", {}).get("entries", [])

    posts = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        content = entry.get("content") or {}
        tweet = content.get("tweet") if isinstance(content, dict) else None
        if not isinstance(tweet, dict):
            continue

        tweet_id = str(tweet.get("id_str") or tweet.get("id") or "").strip()
        full_text = str(
            tweet.get("full_text")
            or tweet.get("text")
            or ""
        ).strip()

        if not tweet_id or not full_text:
            continue

        user = tweet.get("user") or {}
        author_handle = str(
            user.get("screen_name") or handle
        ).strip()
        author_name = str(
            user.get("name") or author_handle
        ).strip()

        permalink = str(tweet.get("permalink") or "").strip()
        if permalink.startswith("/"):
            permalink = "https://x.com" + permalink
        elif not permalink.startswith("http"):
            permalink = f"https://x.com/{author_handle}/status/{tweet_id}"

        posts.append(
            {
                "id": tweet_id,
                "text": full_text,
                "link": permalink,
                "published_at": _parse_x_created_at(
                    tweet.get("created_at", "")
                ),
                "author_handle": author_handle,
                "author_name": author_name,
            }
        )

    return posts


def collect_social_posts() -> dict:
    """
    Helberg/Bessent 공개 X 계정을 무료 best-effort 방식으로 센싱합니다.
    새 게시물은 기존 articles/article_matches 테이블에 함께 저장됩니다.
    """
    stats = {
        "profiles_checked": 0,
        "matched_posts": 0,
        "new_posts": 0,
        "errors": 0,
    }

    for category_name, accounts in SOCIAL_ACCOUNTS.items():
        for account in accounts:
            handle = account["handle"]
            label = account["label"]

            try:
                posts = _fetch_x_profile_timeline(handle)
                stats["profiles_checked"] += 1
            except Exception as exc:
                print(f"[Social feed error] @{handle}: {exc}")
                stats["errors"] += 1
                continue

            for post in posts:
                stats["matched_posts"] += 1

                # 제목은 목록 가독성을 위해 짧게, 원문은 description에 전체 저장
                one_line = re.sub(r"\s+", " ", post["text"]).strip()
                title = (
                    one_line[:150] + "…"
                    if len(one_line) > 150
                    else one_line
                )

                try:
                    _, is_new, _ = upsert_article(
                        category_name=category_name,
                        title=title,
                        link=post["link"],
                        source=f"X · @{handle}",
                        published_at=post["published_at"],
                        description=post["text"],
                        matched_keywords=[f"@{handle}"],
                    )

                    if is_new:
                        stats["new_posts"] += 1

                except Exception as exc:
                    print(f"[Social save error] @{handle}: {exc}")
                    stats["errors"] += 1

    return stats


def summarize_pending_articles(limit: int = 10) -> dict:
    """
    가장 최근의 미요약 콘텐츠를 우선 자동 요약하되,
    하루 AUTO_SUMMARY_DAILY_LIMIT(기본 10회)을 절대 넘지 않습니다.
    """
    stats = {
        "checked": 0,
        "summarized": 0,
        "failed": 0,
        "skipped_by_daily_limit": 0,
        "external_quota_exhausted": False,
    }

    remaining_auto = remaining_summary_quota("auto")
    allowed = min(limit, remaining_auto)

    if allowed <= 0:
        stats["skipped_by_daily_limit"] = limit
        return stats

    with SessionLocal() as session:
        pending = session.scalars(
            select(Article)
            .where(Article.summary.is_(None))
            .order_by(Article.detected_at.desc())
            .limit(allowed)
        ).all()

        items = [
            {
                "id": article.id,
                "title": article.title,
                "description": article.description,
            }
            for article in pending
        ]

    for item in items:
        if remaining_summary_quota("auto") <= 0:
            break

        stats["checked"] += 1
        summary = summarize_article(
            title=item["title"],
            description=item["description"],
        )

        if summary:
            update_article_summary(item["id"], summary)
            record_summary_usage("auto", item["id"])
            stats["summarized"] += 1
        else:
            stats["failed"] += 1
            if _LAST_SUMMARY_ERROR == "quota":
                stats["external_quota_exhausted"] = True
                break

    return stats

# =========================================================
# 6. 화면용 조회
# =========================================================


def _article_in_allowed_government_scope(
    link: str,
    source: str,
) -> bool:
    """
    이미 DB에 저장된 과거 직접수집 항목도 화면에서 정부 섹션 범위를 지킵니다.
    - State Department: /releases/ 만
    - White House: News 허브에 속하는 6개 콘텐츠 경로만

    news.google.com redirect처럼 실제 원문 경로를 알 수 없는 과거 항목은
    기존 데이터 보존을 위해 그대로 둡니다.
    """
    host = _host_from_url(link)

    if host == "state.gov":
        path = urllib.parse.urlparse(
            link
        ).path or "/"
        return path.startswith("/releases/")

    if host == "whitehouse.gov":
        path = urllib.parse.urlparse(
            link
        ).path or "/"
        allowed = (
            "/releases/",
            "/briefings-statements/",
            "/presidential-actions/",
            "/fact-sheets/",
            "/remarks/",
            "/research/",
        )
        return any(
            path.startswith(prefix)
            for prefix in allowed
        )

    return True

def get_category_articles(
    category_name: str,
    period_hours: int | None = 48,
    time_basis: str = "detected",
    limit: int = 100,
):
    with SessionLocal() as session:
        category = session.scalar(
            select(Category).where(Category.name == category_name)
        )
        if not category:
            return []

        article_ids = (
            select(ArticleMatch.article_id)
            .where(ArticleMatch.category_id == category.id)
            .distinct()
        )

        stmt = select(Article).where(Article.id.in_(article_ids))

        if period_hours is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=period_hours)

            if time_basis == "published":
                # V3.2:
                # State.gov 같은 일부 출처가 자동접근을 막아 발행시각을
                # 추출하지 못한 경우 detected_at을 fallback으로 사용합니다.
                effective_time = func.coalesce(
                    Article.published_at,
                    Article.detected_at,
                )
                stmt = stmt.where(
                    effective_time >= cutoff
                )
            else:
                # 기존 기본값: 우리 시스템이 처음 발견한 시각 기준
                stmt = stmt.where(Article.detected_at >= cutoff)

        # 선택한 시간 기준에 맞춰 최신순 정렬
        if time_basis == "published":
            effective_time = func.coalesce(
                Article.published_at,
                Article.detected_at,
            )
            stmt = stmt.order_by(
                effective_time.desc(),
                Article.detected_at.desc(),
            )
        else:
            stmt = stmt.order_by(
                Article.detected_at.desc(),
                Article.published_at.desc().nullslast(),
            )

        stmt = stmt.limit(limit)

        articles = session.scalars(stmt).all()

        result = []

        for article in articles:
            if not _article_in_allowed_government_scope(
                article.link,
                article.source,
            ):
                continue

            tags = session.scalars(
                select(ArticleMatch.keyword)
                .where(
                    ArticleMatch.article_id == article.id,
                    ArticleMatch.category_id == category.id,
                )
                .order_by(ArticleMatch.id)
            ).all()

            result.append(
                {
                    "id": article.id,
                    "title": article.title,
                    "link": article.link,
                    "source": article.source,
                    "published_at": article.published_at,
                    "detected_at": article.detected_at,
                    "description": article.description,
                    "summary": article.summary,
                    "tags": list(tags),
                }
            )

        return result


def format_kst(dt: datetime | None) -> str:
    if not dt:
        return "시간 정보 없음"

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(KST).strftime("%m월 %d일 %H:%M (KST)")


def count_articles() -> int:
    with SessionLocal() as session:
        return session.scalar(select(func.count(Article.id))) or 0


# =========================================================
# 7. Streamlit 관리자 설정
# =========================================================

def render_sidebar_settings():
    st.sidebar.header("⚙️ 대시보드 설정")

    admin_password = os.environ.get("ADMIN_PASSWORD", "")

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False

    if admin_password:
        if not st.session_state.admin_authenticated:
            entered = st.sidebar.text_input(
                "관리자 비밀번호",
                type="password",
                key="admin_password_input",
            )

            if st.sidebar.button("🔐 설정 열기", use_container_width=True):
                if hmac.compare_digest(entered, admin_password):
                    st.session_state.admin_authenticated = True
                    st.rerun()
                else:
                    st.sidebar.error("비밀번호가 맞지 않습니다.")
            return
    else:
        st.sidebar.warning(
            "ADMIN_PASSWORD가 아직 없습니다. "
            "배포 전 Railway 변수에 관리자 비밀번호를 꼭 등록하세요."
        )
        st.session_state.admin_authenticated = True

    if st.session_state.admin_authenticated:
        if admin_password and st.sidebar.button(
            "🔒 설정 닫기", use_container_width=True
        ):
            st.session_state.admin_authenticated = False
            st.rerun()

        keyword_map, domains = get_settings()

        st.sidebar.caption(
            "키워드는 한 줄에 하나씩 입력하세요. "
            "`AI Policy`는 하나의 연속 구문으로 검색하며 "
            "`AI Policy / AI Policies`를 자동으로 함께 찾습니다. "
            '`"AI" AND "Data Center"`처럼 입력하면 AI와 '
            "`Data Center / Data Centers`가 기사 안에 모두 있어야 검색됩니다."
        )

        with st.sidebar.form("settings_form"):
            keyword_inputs = {}

            for category_name in NEWS_CATEGORY_NAMES:
                with st.expander(f"🔎 {category_name} 키워드", expanded=False):
                    keyword_inputs[category_name] = st.text_area(
                        f"{category_name} 키워드",
                        value="\n".join(keyword_map.get(category_name, [])),
                        height=170,
                        key=f"settings_{category_name}",
                        label_visibility="collapsed",
                    )

            domains_input = st.text_area(
                "🌐 탐색할 언론사 사이트",
                value="\n".join(domains),
                height=180,
                help="한 줄에 한 도메인을 입력하세요. 예: reuters.com",
            )

            submitted = st.form_submit_button(
                "💾 설정 영구 저장",
                use_container_width=True,
            )

            if submitted:
                new_keyword_map = {
                    name: parse_multiline_values(keyword_inputs[name])
                    for name in NEWS_CATEGORY_NAMES
                }
                new_domains = parse_multiline_values(domains_input)

                try:
                    save_settings(new_keyword_map, new_domains)
                    st.success("설정이 DB에 영구 저장되었습니다.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"저장 실패: {exc}")


# =========================================================
# 8. Streamlit 메인 화면
# =========================================================

def render_article_card(article: dict, category_name: str):
    source = article["source"] or "Unknown"
    title = article["title"] or "(제목 없음)"
    tags = article["tags"]

    st.markdown(f"### ⭐ {source} | {title}")

    if tags:
        hashtag_display = " ".join(
            f"`#{tag.replace(' ', '_')}`" for tag in tags
        )
        st.markdown(hashtag_display)

    col1, col2 = st.columns(2)
    with col1:
        st.caption(f"🗞️ 기사 발행: {format_kst(article['published_at'])}")
    with col2:
        st.caption(f"📡 최초 감지: {format_kst(article['detected_at'])}")

    if article["summary"]:
        st.markdown("**🤖 3줄 요약**")
        st.markdown(article["summary"])
    else:
        st.info(
            "🤖 아직 3줄 요약이 생성되지 않은 기사입니다. "
            "아래 버튼을 누르면 지금 바로 생성할 수 있습니다. "
            "자동 요약은 하루 최대 10개이며, 나머지는 필요한 기사만 수동으로 요약할 수 있습니다."
        )
        if st.button(
            "🤖 이 기사 3줄 요약 생성",
            key=f"summary_{category_name}_{article['id']}",
        ):
            if remaining_summary_quota("manual") <= 0:
                st.warning(
                    "오늘 이 사이트의 수동 요약 10회를 모두 사용했습니다. "
                    "Gemini 일일 한도가 리셋된 뒤 다시 사용할 수 있습니다."
                )
            else:
                with st.spinner("요약 중..."):
                    summary = summarize_article(
                        article["title"],
                        article["description"],
                    )
                    if summary:
                        update_article_summary(article["id"], summary)
                        record_summary_usage("manual", article["id"])
                        st.rerun()
                    elif _LAST_SUMMARY_ERROR == "quota":
                        st.error(
                            "Gemini 프로젝트의 실제 무료 일일 한도가 소진되었습니다. "
                            "기존 사이트도 같은 API 프로젝트를 사용하면 사용량을 함께 공유합니다."
                        )
                    else:
                        st.error(
                            "요약 생성에 실패했습니다. 잠시 후 다시 시도해주세요."
                        )

    st.link_button("🔗 기사 원문 보러가기", article["link"])
    st.divider()


def render_social_card(article: dict, category_name: str):
    """소셜 탭 전용 카드."""
    source = article["source"] or "X"
    tags = article["tags"]

    st.markdown(f"### 🗣️ {source}")

    if tags:
        st.markdown(
            " ".join(f"`{tag}`" for tag in tags)
        )

    st.markdown(article["description"] or article["title"])

    col1, col2 = st.columns(2)
    with col1:
        st.caption(
            f"🕒 게시: {format_kst(article['published_at'])}"
        )
    with col2:
        st.caption(
            f"📡 최초 감지: {format_kst(article['detected_at'])}"
        )

    if article["summary"]:
        st.markdown("**🤖 3줄 요약**")
        st.markdown(article["summary"])
    else:
        st.info(
            "🤖 아직 요약이 생성되지 않은 공개 소셜 게시물입니다. "
            "자동 요약은 뉴스와 소셜을 합쳐 하루 최대 10개이며, "
            "필요한 게시물은 수동 요약 버튼으로 확인할 수 있습니다."
        )

        if st.button(
            "🤖 이 게시물 3줄 요약 생성",
            key=f"social_summary_{category_name}_{article['id']}",
        ):
            if remaining_summary_quota("manual") <= 0:
                st.warning(
                    "오늘 이 사이트의 수동 요약 10회를 모두 사용했습니다."
                )
            else:
                with st.spinner("요약 중..."):
                    summary = summarize_article(
                        article["title"],
                        article["description"],
                    )
                    if summary:
                        update_article_summary(article["id"], summary)
                        record_summary_usage("manual", article["id"])
                        st.rerun()
                    elif _LAST_SUMMARY_ERROR == "quota":
                        st.error(
                            "Gemini 프로젝트의 실제 무료 일일 한도가 소진되었습니다."
                        )
                    else:
                        st.error(
                            "요약 생성에 실패했습니다. 잠시 후 다시 시도해주세요."
                        )

    st.link_button("🔗 X 원문 보기", article["link"])
    st.divider()


def main():
    st.set_page_config(
        page_title="GPA 뉴스 센싱 대시보드 V3.2.2",
        page_icon="📰",
        layout="wide",
    )

    init_db()

    st.title("📰 글로벌 대외협력(GPA) 뉴스 센싱 대시보드 V3")
    st.caption(
        "Keyword-First Hybrid: 언론사 검색 + State Press Releases + White House News 전용 센싱 / "
        "키워드·언론사 영구 저장 / 뉴스 5개 + 소셜 3개 탭 / "
        "정확 구문·AND 검색 / Gemini 3줄 요약"
    )

    render_sidebar_settings()

    top1, top2 = st.columns([1, 3])

    with top1:
        if st.button("🔄 지금 새 뉴스·소셜 수집", use_container_width=True):
            with st.spinner(
                "Hybrid 방식으로 지정 매체·Google News·공개 소셜을 확인하고 있습니다..."
            ):
                stats = collect_all_categories(
                    generate_summaries=False
                )
                social_stats = collect_social_posts()

            direct = stats.get("direct", {})
            native = stats.get("native", {})

            st.success(
                f"Hybrid 수집 완료: 신규 기사 {stats['new_articles']}개 "
                f"(최신페이지 {direct.get('new_articles', 0)} / "
                f"사이트검색 {native.get('new_articles', 0)} / "
                f"Google {stats.get('google_new_articles', 0)}) · "
                f"신규 소셜 {social_stats['new_posts']}개"
            )

            st.rerun()

    with top2:
        quota = get_summary_quota_status()
        st.caption(
            f"현재 DB 저장 기사: {count_articles()}개 · "
            f"오늘 AI 요약: 자동 {quota['auto_used']}/{quota['auto_limit']} · "
            f"수동 {quota['manual_used']}/{quota['manual_limit']}"
        )

    keyword_map, _ = get_settings()

    period_options = {
        "최근 12시간": 12,
        "최근 24시간": 24,
        "최근 48시간": 48,
        "최근 7일": 24 * 7,
        "최근 30일": 24 * 30,
        "전체": None,
    }

    time_basis_options = {
        "기사 발행/소셜 게시 시각": "published",
        "최초 감지 시각": "detected",
    }

    # 모든 탭이 함께 쓰는 공통 필터.
    # 한 번 선택하면 다른 탭으로 이동해도 값이 유지됩니다.
    filter_col1, filter_col2 = st.columns(2)

    with filter_col1:
        selected_time_basis = st.selectbox(
            "기간 기준",
            list(time_basis_options.keys()),
            index=0,
            key="global_time_basis",
        )

    with filter_col2:
        selected_period = st.selectbox(
            "기간",
            list(period_options.keys()),
            index=2,
            key="global_period",
        )

    tabs = st.tabs(CATEGORY_NAMES)

    for tab, category_name in zip(tabs, CATEGORY_NAMES):
        with tab:
            is_social = category_name in SOCIAL_CATEGORY_NAMES

            if is_social:
                account_labels = [
                    f"{a['label']} (@{a['handle']})"
                    for a in SOCIAL_ACCOUNTS.get(category_name, [])
                ]
                st.caption(
                    "무료 공개 X 센싱 대상: "
                    + " · ".join(account_labels)
                )
                st.caption(
                    "※ X의 공개 임베드 타임라인을 이용한 무료 best-effort 방식이라 "
                    "X가 구조를 변경하면 일시적으로 수집이 중단될 수 있습니다."
                )
            else:
                current_keywords = keyword_map.get(category_name, [])

                if current_keywords:
                    st.caption(
                        "현재 키워드: "
                        + " · ".join(f'"{kw}"' for kw in current_keywords)
                    )
                else:
                    st.warning(
                        f"{category_name} 탭에 등록된 키워드가 없습니다. "
                        "관리자 설정에서 키워드를 추가해주세요."
                    )
                    continue

            articles = get_category_articles(
                category_name,
                period_hours=period_options[selected_period],
                time_basis=time_basis_options[selected_time_basis],
                limit=100,
            )

            if not articles:
                if is_social:
                    st.info(
                        "아직 저장된 공개 소셜 게시물이 없습니다. "
                        "상단의 '지금 새 뉴스·소셜 수집'을 누르거나 "
                        "30분 자동 수집을 기다려주세요."
                    )
                else:
                    st.info(
                        "해당 기간에 저장된 기사가 없습니다. "
                        "상단의 '지금 새 뉴스·소셜 수집'을 눌러 먼저 검색해보세요."
                    )
                continue

            label = "게시물" if is_social else "기사"
            st.success(f"조건에 맞는 {label} {len(articles)}개")

            for article in articles:
                if is_social:
                    render_social_card(article, category_name)
                else:
                    render_article_card(article, category_name)


if __name__ == "__main__":
    main()
