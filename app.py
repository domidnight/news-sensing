import os
import re
import hmac
import hashlib
import json
import urllib.parse
import email.utils
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
SOCIAL_CATEGORY_NAMES = ["소셜 (Helberg)", "소셜 (Bessent)"]
CATEGORY_NAMES = NEWS_CATEGORY_NAMES + SOCIAL_CATEGORY_NAMES

# 무료 공개 X 임베드 타임라인 기반 센싱 대상
# Helberg는 국무부 공식 직책 계정 + 개인 공개 계정을 함께 확인합니다.
SOCIAL_ACCOUNTS = {
    "소셜 (Helberg)": [
        {"handle": "UnderSecE", "label": "Jacob S. Helberg · 국무부 공식"},
        {"handle": "jacobhelberg", "label": "Jacob Helberg · 개인 공개"},
    ],
    "소셜 (Bessent)": [
        {"handle": "SecScottBessent", "label": "Scott Bessent · 재무장관 공식"},
    ],
}
KST = timezone(timedelta(hours=9))

# Google News 검색 시 최근 몇 시간을 볼지 설정
SEARCH_LOOKBACK_HOURS = int(os.environ.get("SEARCH_LOOKBACK_HOURS", "48"))

# Gemini 모델명은 Railway 변수에서 바꿀 수 있음
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")

RSS_TIMEOUT_SECONDS = int(os.environ.get("RSS_TIMEOUT_SECONDS", "10"))
RSS_MAX_WORKERS = int(os.environ.get("RSS_MAX_WORKERS", "10"))

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


def exact_phrase_match(keyword: str, text: str) -> bool:
    """
    핵심 요구사항:
    사용자가 'A B'를 등록했으면 A만, B만, A ... B는 잡지 않고
    'A B'라는 연속 구문만 잡습니다.

    단, A와 B 사이의 공백 개수 차이는 허용합니다.
    """
    keyword = normalize_keyword(keyword)
    if not keyword:
        return False

    clean_text = BeautifulSoup(text or "", "html.parser").get_text(" ", strip=True)
    clean_text = re.sub(r"\s+", " ", clean_text)

    parts = keyword.split(" ")
    phrase = r"\s+".join(re.escape(part) for part in parts)

    # 단어의 일부로 들어간 경우까지 잡지 않도록 경계 검사
    pattern = rf"(?<!\w){phrase}(?!\w)"
    return re.search(pattern, clean_text, flags=re.IGNORECASE) is not None


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
            if not article.description and description:
                article.description = description
            if (not article.source or article.source == "Unknown") and source:
                article.source = source

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
    generate_summaries=False:
        웹사이트 수동 버튼용. RSS만 빠르게 병렬 수집하고 요약은 기다리지 않습니다.

    generate_summaries=True:
        자동 collector용. RSS 수집 후 요약이 없는 기사까지 Gemini로 처리합니다.
    """
    keyword_map, domains = get_settings()

    stats = {
        "feeds_checked": 0,
        "matched_articles": 0,
        "new_articles": 0,
        "summaries_created": 0,
        "errors": 0,
    }

    chunk_size = 4
    jobs = []

    for category_name in NEWS_CATEGORY_NAMES:
        category_keywords = keyword_map.get(category_name, [])
        if not category_keywords:
            continue

        chunks = [
            category_keywords[i:i + chunk_size]
            for i in range(0, len(category_keywords), chunk_size)
        ]

        for domain in domains:
            for chunk in chunks:
                query_parts = [f'"{kw}"' for kw in chunk]
                kw_query = " OR ".join(query_parts)
                full_query = (
                    f"({kw_query}) site:{domain} "
                    f"when:{SEARCH_LOOKBACK_HOURS}h"
                )
                encoded_query = urllib.parse.quote(full_query)
                rss_url = (
                    "https://news.google.com/rss/search"
                    f"?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"
                )
                jobs.append(
                    {
                        "category_name": category_name,
                        "category_keywords": category_keywords,
                        "domain": domain,
                        "rss_url": rss_url,
                    }
                )

    feed_results = []
    if jobs:
        workers = max(1, min(RSS_MAX_WORKERS, len(jobs)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_fetch_feed_job, job) for job in jobs]
            for future in as_completed(futures):
                result = future.result()
                feed_results.append(result)
                stats["feeds_checked"] += 1
                if result["error"]:
                    print(f"[Feed error] {result['domain']}: {result['error']}")
                    stats["errors"] += 1

    seen_links_by_category = {
        category_name: set() for category_name in NEWS_CATEGORY_NAMES
    }
    summary_queue = {}

    for result in feed_results:
        category_name = result["category_name"]
        category_keywords = result["category_keywords"]

        for entry in result["entries"]:
            title = str(entry.get("title", "") or "")
            description_raw = str(entry.get("description", "") or "")
            link = str(entry.get("link", "") or "")

            if not link or link in seen_links_by_category[category_name]:
                continue

            text_to_search = f"{title} {description_raw}"
            matched_keywords = [
                kw for kw in category_keywords
                if exact_phrase_match(kw, text_to_search)
            ]

            if not matched_keywords:
                continue

            seen_links_by_category[category_name].add(link)
            stats["matched_articles"] += 1

            source = get_feed_source(entry)
            published_at = parse_published(str(entry.get("published", "") or ""))
            clean_description = BeautifulSoup(
                description_raw, "html.parser"
            ).get_text(" ", strip=True)

            try:
                article_id, is_new, needs_summary = upsert_article(
                    category_name=category_name,
                    title=title,
                    link=link,
                    source=source,
                    published_at=published_at,
                    description=clean_description,
                    matched_keywords=matched_keywords,
                )

                if is_new:
                    stats["new_articles"] += 1

                if article_id and needs_summary and generate_summaries:
                    summary_queue[article_id] = (title, clean_description)

            except Exception as exc:
                print(f"[Article save error] {exc}")
                stats["errors"] += 1

    stats["auto_quota_exhausted"] = False

    if generate_summaries:
        remaining_auto = remaining_summary_quota("auto")

        for article_id, (title, clean_description) in summary_queue.items():
            if remaining_auto <= 0:
                break

            summary = summarize_article(
                title=title,
                description=clean_description,
            )
            if summary:
                update_article_summary(article_id, summary)
                record_summary_usage("auto", article_id)
                stats["summaries_created"] += 1
                remaining_auto -= 1
            elif _LAST_SUMMARY_ERROR == "quota":
                stats["auto_quota_exhausted"] = True
                break

    return stats


def _parse_x_created_at(raw_value: str) -> datetime | None:
    """X syndication의 created_at 문자열을 UTC datetime으로 변환."""
    raw_value = str(raw_value or "").strip()
    if not raw_value:
        return None

    try:
        dt = datetime.strptime(
            raw_value,
            "%a %b %d %H:%M:%S %z %Y",
        )
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        dt = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


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

def get_category_articles(
    category_name: str,
    period_hours: int | None = 48,
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
            stmt = stmt.where(Article.detected_at >= cutoff)

        stmt = stmt.order_by(
            Article.published_at.desc().nullslast(),
            Article.detected_at.desc(),
        ).limit(limit)

        articles = session.scalars(stmt).all()

        result = []

        for article in articles:
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
            "예: `A B`는 A 또는 B가 아니라 `A B`라는 구문 전체로 검색됩니다."
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
        page_title="GPA 뉴스 센싱 대시보드 V2",
        page_icon="📰",
        layout="wide",
    )

    init_db()

    st.title("📰 글로벌 대외협력(GPA) 뉴스 센싱 대시보드 V2")
    st.caption(
        "키워드·언론사 영구 저장 / 뉴스 5개 + 소셜 2개 탭 / 정확 구문 검색 / "
        "기사·공개 소셜 DB 저장 / 최초 감지 시각 / Gemini 3줄 요약"
    )

    render_sidebar_settings()

    top1, top2 = st.columns([1, 3])

    with top1:
        if st.button("🔄 지금 새 뉴스·소셜 수집", use_container_width=True):
            with st.spinner("뉴스와 공개 소셜 계정을 빠르게 확인하고 있습니다..."):
                stats = collect_all_categories(generate_summaries=False)
                social_stats = collect_social_posts()

            st.success(
                f"빠른 수집 완료: 신규 기사 {stats['new_articles']}개 / "
                f"신규 소셜 {social_stats['new_posts']}개 / "
                f"오류 {stats['errors'] + social_stats['errors']}건"
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
    tabs = st.tabs(CATEGORY_NAMES)

    period_options = {
        "최근 24시간": 24,
        "최근 48시간": 48,
        "최근 7일": 24 * 7,
        "최근 30일": 24 * 30,
        "전체": None,
    }

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

            selected_period = st.selectbox(
                "기간 (최초 감지 시각 기준)",
                list(period_options.keys()),
                index=1,
                key=f"period_{category_name}",
            )

            articles = get_category_articles(
                category_name,
                period_hours=period_options[selected_period],
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
