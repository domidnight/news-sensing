import os
import re
import hmac
import hashlib
import urllib.parse
import email.utils
from datetime import datetime, timezone, timedelta

import feedparser
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

CATEGORY_NAMES = ["AI", "국무부", "국방부", "텍사스", "관세"]
KST = timezone(timedelta(hours=9))

# Google News 검색 시 최근 몇 시간을 볼지 설정
SEARCH_LOOKBACK_HOURS = int(os.environ.get("SEARCH_LOOKBACK_HOURS", "48"))

# Gemini 모델명은 Railway 변수에서 바꿀 수 있음
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.7-flash")

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


def init_db():
    """DB 테이블을 만들고, 최초 실행 시에만 기본 설정을 입력합니다."""
    Base.metadata.create_all(engine)

    with SessionLocal() as session:
        category_count = session.scalar(select(func.count(Category.id))) or 0

        # DB가 완전히 처음 만들어졌을 때만 기본값 입력
        if category_count == 0:
            category_objects = {}

            for idx, category_name in enumerate(CATEGORY_NAMES):
                obj = Category(name=category_name, sort_order=idx)
                session.add(obj)
                session.flush()
                category_objects[category_name] = obj

                for kw in DEFAULT_KEYWORDS.get(category_name, []):
                    session.add(
                        Keyword(category_id=obj.id, keyword=normalize_keyword(kw))
                    )

            for domain in DEFAULT_DOMAINS:
                session.add(Source(domain=normalize_domain(domain), enabled=True))

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

        for category_name in CATEGORY_NAMES:
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


# =========================================================
# 4. Gemini 3줄 요약
# =========================================================

def summarize_article(title: str, description: str) -> str | None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None

    clean_description = BeautifulSoup(
        description or "", "html.parser"
    ).get_text(" ", strip=True)
    clean_description = clean_description[:2500]

    prompt = f"""
아래 뉴스의 제목과 Google News RSS에 포함된 기사 설명을 바탕으로,
글로벌 대외협력(GPA) 담당자가 빠르게 핵심을 파악할 수 있도록 한국어로 요약해 주세요.

규칙:
- 정확히 3개의 짧은 불릿으로 작성
- 확인되지 않은 내용을 추측하지 말 것
- 회사명, 기관명, 정책명 등 핵심 고유명사는 가능하면 유지
- 각 불릿은 한 문장 정도로 간결하게 작성

제목:
{title}

기사 설명:
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


def collect_all_categories() -> dict:
    """
    설정된 5개 카테고리와 언론사를 모두 검색합니다.
    이 함수는 app.py의 수동 새로고침에서도 쓰고,
    collector.py의 30분 자동 실행에서도 그대로 씁니다.
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

    for category_name in CATEGORY_NAMES:
        category_keywords = keyword_map.get(category_name, [])
        if not category_keywords:
            continue

        chunks = [
            category_keywords[i:i + chunk_size]
            for i in range(0, len(category_keywords), chunk_size)
        ]

        # 같은 카테고리에서 동일 기사가 여러 chunk로 중복 처리되지 않게 함
        seen_links_for_category = set()

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

                try:
                    feed = feedparser.parse(rss_url)
                    stats["feeds_checked"] += 1
                except Exception as exc:
                    print(f"[Feed error] {domain}: {exc}")
                    stats["errors"] += 1
                    continue

                for entry in getattr(feed, "entries", []):
                    title = str(entry.get("title", "") or "")
                    description_raw = str(entry.get("description", "") or "")
                    link = str(entry.get("link", "") or "")

                    if not link or link in seen_links_for_category:
                        continue

                    text_to_search = f"{title} {description_raw}"

                    # 중요:
                    # 각 키워드를 "하나의 정확 구문"으로 다시 검사
                    matched_keywords = [
                        kw for kw in category_keywords
                        if exact_phrase_match(kw, text_to_search)
                    ]

                    if not matched_keywords:
                        continue

                    seen_links_for_category.add(link)
                    stats["matched_articles"] += 1

                    source = get_feed_source(entry)
                    published_at = parse_published(
                        str(entry.get("published", "") or "")
                    )

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

                        # 신규 기사 또는 과거에 요약이 실패했던 기사라면 다시 요약 시도
                        if article_id and needs_summary:
                            summary = summarize_article(
                                title=title,
                                description=clean_description,
                            )
                            if summary:
                                update_article_summary(article_id, summary)
                                stats["summaries_created"] += 1

                    except Exception as exc:
                        print(f"[Article save error] {exc}")
                        stats["errors"] += 1

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

            for category_name in CATEGORY_NAMES:
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
                    for name in CATEGORY_NAMES
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
            "3줄 요약이 아직 없습니다. "
            "GEMINI_API_KEY를 확인한 뒤 아래 버튼으로 생성할 수 있습니다."
        )
        if st.button(
            "🤖 이 기사 3줄 요약 생성",
            key=f"summary_{category_name}_{article['id']}",
        ):
            with st.spinner("요약 중..."):
                summary = summarize_article(
                    article["title"],
                    article["description"],
                )
                if summary:
                    update_article_summary(article["id"], summary)
                    st.rerun()
                else:
                    st.error(
                        "요약 생성에 실패했습니다. GEMINI_API_KEY 또는 모델 설정을 확인해주세요."
                    )

    st.link_button("🔗 기사 원문 보러가기", article["link"])
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
        "키워드·언론사 영구 저장 / 5개 카테고리 / 정확 구문 검색 / "
        "기사 DB 저장 / 최초 감지 시각 / Gemini 3줄 요약"
    )

    render_sidebar_settings()

    top1, top2 = st.columns([1, 3])

    with top1:
        if st.button("🔄 지금 새 뉴스 수집", use_container_width=True):
            with st.spinner("등록된 키워드와 언론사를 검색하고 있습니다..."):
                stats = collect_all_categories()

            st.success(
                f"수집 완료: 신규 기사 {stats['new_articles']}개 / "
                f"요약 생성 {stats['summaries_created']}개"
            )
            st.rerun()

    with top2:
        st.caption(
            f"현재 DB 저장 기사: {count_articles()}개 · "
            f"자동 수집은 collector.py를 Railway Cron으로 30분마다 실행합니다."
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
                st.info(
                    "해당 기간에 저장된 기사가 없습니다. "
                    "상단의 '지금 새 뉴스 수집'을 눌러 먼저 검색해보세요."
                )
                continue

            st.success(f"조건에 맞는 기사 {len(articles)}개")

            for article in articles:
                render_article_card(article, category_name)


if __name__ == "__main__":
    main()
