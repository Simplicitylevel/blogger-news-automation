#!/usr/bin/env python3
"""Automated Blogger publisher for Google News India RSS trends.

This script:
1. Pulls trending stories from Google News RSS (India English).
2. Builds SEO-friendly, category-based articles with ad placements.
3. Publishes posts to Blogger with labels and publish logs.

For unattended GitHub Actions runs, provide:
- credentials.json in the workspace, and
- a refresh-capable OAuth token via token.json or BLOGGER_TOKEN_JSON.
"""

import base64
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import quote_plus

import feedparser
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


BLOG_ID = "874281817792739537"
SCOPES = ["https://www.googleapis.com/auth/blogger"]
GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss?hl=en-IN&gl=IN&ceid=IN:en"

POSTS_PER_CATEGORY = 3
REQUEST_TIMEOUT = 30
PUBLISH_DELAY_SECONDS = 2
STATE_FILE = os.environ.get("BLOGGER_STATE_FILE", "published_state.json")
CREDENTIALS_FILE = os.environ.get("BLOGGER_CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE = os.environ.get("BLOGGER_TOKEN_FILE", "token.json")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
IST = timezone(timedelta(hours=5, minutes=30))

NATIVE_BANNER_CODE = """
<script async="async" data-cfasync="false" src="https://pl29001964.profitablecpmratenetwork.com/ed59753cd6ef4716da01077b685e70de/invoke.js"></script>
<div id="container-ed59753cd6ef4716da01077b685e70de"></div>
""".strip()

BANNER_AD_CODE = """
<script>
  atOptions = {
    'key' : '7fbe4abebd1590747e9fa702c8d6a2a5',
    'format' : 'iframe',
    'height' : 90,
    'width' : 728,
    'params' : {}
  };
</script>
<script src="https://www.highperformanceformat.com/7fbe4abebd1590747e9fa702c8d6a2a5/invoke.js"></script>
""".strip()

CATEGORIES = ["Finance", "Government", "Jobs", "Technology", "Business"]

CATEGORY_KEYWORDS = {
    "Finance": [
        "finance",
        "stock",
        "stocks",
        "market",
        "markets",
        "bank",
        "banking",
        "loan",
        "interest rate",
        "inflation",
        "rupee",
        "forex",
        "mutual fund",
        "nifty",
        "sensex",
        "bond",
        "investment",
        "fii",
        "rbi",
    ],
    "Government": [
        "government",
        "ministry",
        "cabinet",
        "parliament",
        "policy",
        "bill",
        "scheme",
        "regulation",
        "supreme court",
        "high court",
        "election",
        "state govt",
        "union government",
        "public sector",
        "department",
    ],
    "Jobs": [
        "jobs",
        "job",
        "recruitment",
        "vacancy",
        "vacancies",
        "hiring",
        "employment",
        "exam",
        "result",
        "notification",
        "admit card",
        "apprentice",
        "staff selection",
        "railway",
        "ssc",
        "upsc",
        "career",
    ],
    "Technology": [
        "technology",
        "tech",
        "ai",
        "artificial intelligence",
        "chip",
        "software",
        "app",
        "startup",
        "cyber",
        "internet",
        "cloud",
        "semiconductor",
        "smartphone",
        "mobile",
        "digital",
        "ev",
    ],
    "Business": [
        "business",
        "company",
        "companies",
        "industry",
        "deal",
        "merger",
        "acquisition",
        "manufacturing",
        "retail",
        "trade",
        "exports",
        "imports",
        "sales",
        "profit",
        "revenue",
        "startup",
        "consumer",
    ],
}

CATEGORY_SEARCH_TERMS = {
    "Finance": ["finance india", "stock market india", "banking india"],
    "Government": ["government india", "india policy", "parliament india"],
    "Jobs": ["jobs india", "recruitment india", "employment india"],
    "Technology": ["technology india", "ai india", "startup technology india"],
    "Business": ["business india", "company india", "industry india"],
}

CATEGORY_BRIEF = {
    "Finance": {
        "audience": "investors, borrowers, and market participants",
        "impact": "pricing, liquidity, and portfolio decisions",
        "trend": "capital allocation and financial sentiment",
    },
    "Government": {
        "audience": "citizens, departments, and regulated sectors",
        "impact": "policy execution, compliance, and public services",
        "trend": "administrative priorities and regulatory direction",
    },
    "Jobs": {
        "audience": "job seekers, training providers, and employers",
        "impact": "recruitment plans, eligibility timelines, and applications",
        "trend": "employment demand and talent pipelines",
    },
    "Technology": {
        "audience": "digital users, founders, and enterprise teams",
        "impact": "product adoption, innovation, and infrastructure choices",
        "trend": "digital transformation and competitive positioning",
    },
    "Business": {
        "audience": "companies, suppliers, and consumers",
        "impact": "commercial strategy, execution, and demand signals",
        "trend": "corporate expansion and operating momentum",
    },
}

SECTION_HEADINGS = {
    "Finance": ["Market Context", "Why This Finance Story Matters", "What To Watch Next"],
    "Government": ["Policy Context", "Why This Government Update Matters", "Next Administrative Steps"],
    "Jobs": ["Recruitment Context", "Why This Jobs Update Matters", "What Applicants Should Watch"],
    "Technology": ["Technology Context", "Why This Tech Story Matters", "What Comes Next"],
    "Business": ["Business Context", "Why This Business Story Matters", "Commercial Outlook"],
}


@dataclass
class NewsItem:
    category: str
    headline: str
    summary: str
    source: str
    link: str
    published: str


@dataclass
class ArticleDraft:
    title: str
    html: str
    labels: List[str]
    topic_key: str


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def now_ist() -> datetime:
    return datetime.now(IST)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def strip_html_tags(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    clean = clean.replace("&nbsp;", " ").replace("&amp;", "&")
    return normalize_whitespace(clean)


def safe_text(text: str, fallback: str) -> str:
    text = normalize_whitespace(text)
    return text if text else fallback


def clean_headline(title: str) -> str:
    raw = normalize_whitespace(title)
    if " - " in raw:
        head, tail = raw.rsplit(" - ", 1)
        if tail and len(tail.split()) <= 6:
            return head.strip()
    return raw


def topic_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


def truncate_text(text: str, limit: int) -> str:
    text = normalize_whitespace(text)
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0].strip()
    return cut + "..."


def build_search_feed_url(query: str) -> str:
    encoded_query = quote_plus(query)
    return (
        "https://news.google.com/rss/search?"
        f"q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"
    )


def read_json_file(path: str) -> Dict[str, object]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Could not read %s: %s", path, exc)
        return {}


def write_json_file(path: str, data: Dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(data, file_obj, ensure_ascii=False, indent=2)


def load_state(path: str) -> Dict[str, List[str]]:
    state = read_json_file(path)
    titles = state.get("published_titles", [])
    topics = state.get("published_topics", [])
    if not isinstance(titles, list) or not isinstance(topics, list):
        return {"published_titles": [], "published_topics": []}
    return {
        "published_titles": [str(item) for item in titles][-1000:],
        "published_topics": [str(item) for item in topics][-1000:],
    }


def save_state(path: str, state: Dict[str, List[str]]) -> None:
    payload = {
        "updated_at": now_ist().isoformat(),
        "published_titles": state["published_titles"][-1000:],
        "published_topics": state["published_topics"][-1000:],
    }
    write_json_file(path, payload)


def build_credentials_from_env() -> Optional[Credentials]:
    token_json = os.environ.get("BLOGGER_TOKEN_JSON", "").strip()
    token_b64 = os.environ.get("BLOGGER_TOKEN_B64", "").strip()

    if token_json:
        return Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)

    if token_b64:
        decoded = base64.b64decode(token_b64).decode("utf-8")
        return Credentials.from_authorized_user_info(json.loads(decoded), SCOPES)

    return None


def authenticate_blogger() -> object:
    creds = build_credentials_from_env()

    if not creds and os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        logging.info("Refreshing Blogger access token")
        creds.refresh(Request())
        if not os.environ.get("BLOGGER_TOKEN_JSON"):
            write_json_file(TOKEN_FILE, json.loads(creds.to_json()))

    if not creds or not creds.valid:
        if os.environ.get("GITHUB_ACTIONS") or os.environ.get("CI"):
            raise RuntimeError(
                "No valid Blogger OAuth token found for unattended execution. "
                "Provide token.json or BLOGGER_TOKEN_JSON with a refresh token."
            )

        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(
                "credentials.json was not found. Place it in the working directory "
                "or set BLOGGER_CREDENTIALS_FILE."
            )

        logging.info("Running one-time local OAuth flow for Blogger")
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
        write_json_file(TOKEN_FILE, json.loads(creds.to_json()))

    return build("blogger", "v3", credentials=creds, cache_discovery=False)


def fetch_feed(feed_url: str) -> feedparser.FeedParserDict:
    response = requests.get(
        feed_url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return feedparser.parse(response.content)


def parse_feed_items(parsed_feed: feedparser.FeedParserDict, category: str) -> List[NewsItem]:
    items = []
    for entry in parsed_feed.entries:
        title = clean_headline(getattr(entry, "title", "") or entry.get("title", ""))
        summary = strip_html_tags(getattr(entry, "summary", "") or entry.get("summary", ""))
        source_data = getattr(entry, "source", None) or entry.get("source", {})
        source = ""
        if isinstance(source_data, dict):
            source = source_data.get("title", "")
        source = source or getattr(source_data, "title", "") or "Google News"
        published = (
            getattr(entry, "published", "")
            or entry.get("published", "")
            or getattr(entry, "updated", "")
            or entry.get("updated", "")
        )
        link = getattr(entry, "link", "") or entry.get("link", "")

        if not title or not link:
            continue

        items.append(
            NewsItem(
                category=category,
                headline=title,
                summary=safe_text(summary, "The development is drawing attention across India."),
                source=safe_text(source, "Google News"),
                link=link,
                published=safe_text(published, now_ist().strftime("%d %b %Y, %I:%M %p IST")),
            )
        )
    return items


def category_score(item: NewsItem, category: str) -> int:
    blob = f"{item.headline} {item.summary} {item.source}".lower()
    score = 0
    for keyword in CATEGORY_KEYWORDS[category]:
        if keyword in blob:
            score += 2 if keyword in item.headline.lower() else 1
    if item.category == category:
        score += 2
    return score


def dedupe_news(items: Iterable[NewsItem]) -> List[NewsItem]:
    seen: Set[str] = set()
    unique: List[NewsItem] = []
    for item in items:
        key = topic_key(item.headline)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def fetch_trending_news() -> Dict[str, List[NewsItem]]:
    logging.info("Fetching Google News India trending feed")
    primary_feed = fetch_feed(GOOGLE_NEWS_RSS_URL)
    primary_items = parse_feed_items(primary_feed, "General")

    category_map: Dict[str, List[NewsItem]] = {category: [] for category in CATEGORIES}

    for category in CATEGORIES:
        matches = [item for item in primary_items if category_score(item, category) > 0]
        category_map[category].extend(matches)

        for query in CATEGORY_SEARCH_TERMS[category]:
            if len(dedupe_news(category_map[category])) >= POSTS_PER_CATEGORY * 2:
                break
            try:
                logging.info("Fetching category feed for %s using query: %s", category, query)
                category_feed = fetch_feed(build_search_feed_url(query))
                category_items = parse_feed_items(category_feed, category)
                category_map[category].extend(category_items)
            except requests.RequestException as exc:
                logging.warning("Feed request failed for %s (%s): %s", category, query, exc)

        category_map[category] = dedupe_news(category_map[category])

    return category_map


def select_news_items(
    category_news: Dict[str, List[NewsItem]],
    state: Dict[str, List[str]],
) -> Dict[str, List[NewsItem]]:
    selected: Dict[str, List[NewsItem]] = {category: [] for category in CATEGORIES}
    used_topics = set(state["published_topics"])

    for category in CATEGORIES:
        ranked = sorted(
            category_news[category],
            key=lambda item: (category_score(item, category), item.published),
            reverse=True,
        )
        for item in ranked:
            key = topic_key(item.headline)
            if key in used_topics:
                continue
            selected[category].append(item)
            used_topics.add(key)
            if len(selected[category]) == POSTS_PER_CATEGORY:
                break

        if len(selected[category]) < POSTS_PER_CATEGORY:
            logging.warning(
                "Only found %s unique topic(s) for %s",
                len(selected[category]),
                category,
            )

    return selected


def unique_title(base_title: str, category: str, used_titles: Set[str]) -> str:
    base_title = truncate_text(base_title, 92)
    options = [
        base_title,
        f"{base_title} | {category} Update",
        f"{base_title} in India",
        f"{base_title} | {now_ist().strftime('%d %b %Y')}",
    ]

    for option in options:
        key = topic_key(option)
        if key not in used_titles:
            used_titles.add(key)
            return option

    final_title = f"{base_title} | {category} News {int(time.time())}"
    used_titles.add(topic_key(final_title))
    return final_title


def build_seo_title(item: NewsItem, category: str, used_titles: Set[str]) -> str:
    headline = re.sub(r"\s+", " ", item.headline).strip(" -:")
    if "india" not in headline.lower():
        headline = f"{headline}: Latest {category} Update in India"
    return unique_title(headline, category, used_titles)


def build_article_paragraphs(item: NewsItem, category: str) -> List[str]:
    brief = CATEGORY_BRIEF[category]
    headline = truncate_text(item.headline, 140)
    summary = truncate_text(item.summary, 260)
    source = item.source
    published = item.published

    paragraphs = [
        (
            f"{headline} is emerging as a closely watched {category.lower()} development in India. "
            f"Early coverage from {source} suggests the story has immediate relevance for "
            f"{brief['audience']} because it can influence {brief['impact']}."
        ),
        (
            f"The current update centers on a fast-moving news cycle where official statements, "
            f"market interpretation, and stakeholder reactions are shaping the narrative. "
            f"{summary}"
        ),
        (
            f"At this stage, the report indicates that the issue is gaining visibility beyond the original headline. "
            f"That matters because decisions connected to {category.lower()} topics often build quickly once fresh data, "
            f"policy language, or management commentary reaches the public domain."
        ),
        (
            f"For readers tracking the bigger picture, this story fits into a broader conversation around "
            f"{brief['trend']}. The latest turn may alter expectations about short-term execution, public response, "
            f"or competitive positioning depending on how the next updates unfold."
        ),
        (
            f"The practical importance lies in how institutions and individuals respond over the next few days. "
            f"Businesses, households, and sector observers will likely measure this development against existing plans, "
            f"budgets, and compliance or investment priorities."
        ),
        (
            f"Another key point is timing. News published around {published} can accelerate follow-up announcements, "
            f"expert commentary, or second-order reactions, especially when the underlying topic already has national interest "
            f"or strong relevance for the Indian economy."
        ),
        (
            f"Readers should also watch for additional details from primary institutions, formal releases, and subsequent reporting. "
            f"Those inputs often determine whether the current update remains a one-day headline or develops into a larger trend with "
            f"measurable implications."
        ),
        (
            f"In summary, {headline.lower()} remains a meaningful story for the {category.lower()} space. "
            f"If fresh confirmations, clarifications, or sector responses appear, the issue could become an even more important reference point "
            f"for decision-makers across India."
        ),
    ]
    return paragraphs


def insert_ads_with_structure(paragraphs: List[str], category: str, item: NewsItem) -> str:
    headings = SECTION_HEADINGS[category]
    published_stamp = now_ist().strftime("%d %b %Y, %I:%M %p IST")

    parts = [f"<p><em>Published: {escape(published_stamp)}</em></p>"]
    banner_after = {2, 4, 6}
    heading_before = {1: headings[0], 3: headings[1], 5: headings[2]}

    for index, paragraph in enumerate(paragraphs):
        if index in heading_before:
            parts.append(f"<h2>{escape(heading_before[index])}</h2>")
        parts.append(f"<p>{escape(paragraph)}</p>")
        if index == 0:
            parts.append(NATIVE_BANNER_CODE)
        elif index in banner_after:
            parts.append(BANNER_AD_CODE)

    parts.append(
        (
            f'<p><strong>Source:</strong> <a href="{escape(item.link, quote=True)}" '
            f'target="_blank" rel="noopener noreferrer nofollow">{escape(item.source)}</a></p>'
        )
    )
    return "\n\n".join(parts)


def generate_article(item: NewsItem, category: str, used_titles: Set[str]) -> ArticleDraft:
    title = build_seo_title(item, category, used_titles)
    paragraphs = build_article_paragraphs(item, category)
    html = insert_ads_with_structure(paragraphs, category, item)
    labels = [category, "Auto News", now_ist().strftime("%Y-%m-%d")]
    return ArticleDraft(title=title, html=html, labels=labels, topic_key=topic_key(item.headline))


def publish_to_blogger(service: object, draft: ArticleDraft) -> Dict[str, object]:
    body = {
        "kind": "blogger#post",
        "blog": {"id": BLOG_ID},
        "title": draft.title,
        "content": draft.html,
        "labels": draft.labels,
    }
    return service.posts().insert(blogId=BLOG_ID, body=body, isDraft=False).execute()


def log_post_result(response: Dict[str, object], title: str) -> None:
    post_id = response.get("id", "unknown")
    post_url = response.get("url") or response.get("selfLink", "unavailable")
    logging.info("Published post | id=%s | title=%s | url=%s", post_id, title, post_url)


def main() -> None:
    configure_logging()
    logging.info("Starting Blogger automation run")

    state = load_state(STATE_FILE)
    used_titles = {topic_key(title) for title in state["published_titles"]}

    try:
        service = authenticate_blogger()
    except Exception as exc:
        logging.error("Authentication failed: %s", exc)
        raise

    try:
        category_news = fetch_trending_news()
        selected_news = select_news_items(category_news, state)
    except requests.RequestException as exc:
        logging.error("Failed to fetch news feeds: %s", exc)
        raise

    published_count = 0
    attempted_count = 0

    for category in CATEGORIES:
        items = selected_news.get(category, [])[:POSTS_PER_CATEGORY]
        logging.info("Preparing %s post(s) for category: %s", len(items), category)

        for item in items:
            attempted_count += 1
            draft = generate_article(item, category, used_titles)

            try:
                response = publish_to_blogger(service, draft)
                log_post_result(response, draft.title)
                state["published_titles"].append(draft.title)
                state["published_topics"].append(draft.topic_key)
                save_state(STATE_FILE, state)
                published_count += 1
                time.sleep(PUBLISH_DELAY_SECONDS)
            except HttpError as exc:
                logging.error("Blogger API error for '%s': %s", draft.title, exc)
            except Exception as exc:
                logging.error("Unexpected publish error for '%s': %s", draft.title, exc)

    logging.info(
        "Automation run complete | attempted=%s | published=%s",
        attempted_count,
        published_count,
    )


if __name__ == "__main__":
    main()
