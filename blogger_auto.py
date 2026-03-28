#!/usr/bin/env python3
"""Automated Blogger publisher for Google News India RSS trends.

This script keeps the original automation flow intact while adding:
- long-form 1200-1600 word article generation
- featured image extraction with layered fallbacks
- SEO-friendly sectioned HTML output
- resilient network handling for GitHub Actions automation

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from html import escape, unescape
from html.parser import HTMLParser
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote_plus, urljoin

import feedparser
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BLOG_ID = "874281817792739537"
SCOPES = ["https://www.googleapis.com/auth/blogger"]
GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss?hl=en-IN&gl=IN&ceid=IN:en"

POSTS_PER_CATEGORY = 3
REQUEST_TIMEOUT = 20
SOURCE_FETCH_TIMEOUT = 18
IMAGE_TIMEOUT = 10
PUBLISH_DELAY_SECONDS = 2
MAX_WORKERS = int(os.environ.get("BLOGGER_MAX_WORKERS", "6"))
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

UNSPLASH_FALLBACK_TEMPLATE = "https://source.unsplash.com/featured/1600x900/?{}"

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
        "audience": "investors, borrowers, banks, and market participants",
        "impact": "pricing, liquidity, borrowing costs, and portfolio decisions",
        "trend": "capital allocation, inflation expectations, and financial sentiment",
        "reaction": "banks, treasury desks, investors, and regulators",
        "economy": "credit demand, savings behavior, business funding, and market confidence",
        "experts": "rate expectations, liquidity conditions, earnings visibility, and risk appetite",
        "next_steps": "policy signals, macro data, bond yields, and company disclosures",
    },
    "Government": {
        "audience": "citizens, departments, state agencies, and regulated sectors",
        "impact": "policy execution, compliance, implementation timelines, and public services",
        "trend": "administrative priorities, governance delivery, and regulatory direction",
        "reaction": "ministries, state governments, legal observers, and regulated industries",
        "economy": "public spending, implementation efficiency, service access, and business compliance",
        "experts": "policy clarity, execution capacity, inter-agency coordination, and legal durability",
        "next_steps": "official notifications, implementation rules, parliamentary signals, and court responses",
    },
    "Jobs": {
        "audience": "job seekers, recruiters, exam bodies, and employers",
        "impact": "recruitment plans, eligibility timelines, applications, and talent pipelines",
        "trend": "employment demand, hiring visibility, and workforce readiness",
        "reaction": "candidates, employers, training institutes, and recruitment agencies",
        "economy": "income security, household spending, labor participation, and skill utilization",
        "experts": "application timelines, vacancy quality, exam scheduling, and labor market absorption",
        "next_steps": "official notifications, admit cards, results, and hiring follow-through",
    },
    "Technology": {
        "audience": "digital users, founders, and enterprise teams",
        "impact": "product adoption, infrastructure choices, investment priorities, and innovation plans",
        "trend": "digital transformation, platform competition, and product scaling",
        "reaction": "startups, major platforms, enterprises, and digital policy observers",
        "economy": "productivity, digital access, startup funding, and infrastructure investment",
        "experts": "commercial viability, scale economics, regulation, cybersecurity, and execution risks",
        "next_steps": "product rollouts, regulatory commentary, funding moves, and adoption metrics",
    },
    "Business": {
        "audience": "companies, suppliers, distributors, investors, and consumers",
        "impact": "commercial strategy, operating momentum, execution quality, and demand visibility",
        "trend": "corporate expansion, pricing discipline, and industry competitiveness",
        "reaction": "management teams, investors, competitors, and channel partners",
        "economy": "investment flows, supply chains, employment, and consumer confidence",
        "experts": "margin resilience, execution quality, demand trends, and competitive positioning",
        "next_steps": "management commentary, filings, demand signals, and industry-wide responses",
    },
}

ARTICLE_SECTIONS: Sequence[Tuple[str, int, int]] = (
    ("Introduction", 150, 200),
    ("Background Context", 150, 200),
    ("Latest Developments", 150, 200),
    ("Government and Market Reaction", 150, 200),
    ("Why This Matters", 150, 200),
    ("Impact on India and the Economy", 150, 200),
    ("What Experts Are Saying", 140, 160),
    ("What Happens Next", 140, 160),
    ("Conclusion", 100, 150),
)

NOISY_LINE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"^read more$",
        r"^also read$",
        r"^watch live$",
        r"^watch video$",
        r"^advertisement$",
        r"^follow us on .+$",
        r"^click here .+$",
        r"^subscribe .+$",
        r"^download .+$",
        r"^sign up .+$",
        r"^share this article.*$",
        r"^listen to this article.*$",
    ]
]

IMAGE_BLOCKLIST_HINTS = ("logo", "icon", "sprite", "avatar", "ads", "doubleclick", ".svg")


@dataclass
class NewsItem:
    category: str
    headline: str
    summary: str
    source: str
    link: str
    published: str
    rss_image: str = ""


@dataclass
class SourceDetails:
    description: str
    snippets: List[str]
    og_image: str = ""
    first_image: str = ""


@dataclass
class ArticleDraft:
    title: str
    html: str
    labels: List[str]
    topic_key: str


class SourceHTMLParser(HTMLParser):
    """Small HTML parser for meta tags, image candidates, and paragraph snippets."""

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.meta_tags: List[Dict[str, str]] = []
        self.images: List[str] = []
        self.paragraphs: List[str] = []
        self._capture_paragraph = False
        self._current_paragraph: List[str] = []
        self._capture_title = False
        self._blocked_depth = 0

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_map = {key.lower(): value or "" for key, value in attrs}
        tag = tag.lower()

        if tag in {"script", "style", "noscript"}:
            self._blocked_depth += 1
            return

        if tag == "meta":
            self.meta_tags.append(attr_map)
            return

        if tag == "img":
            src = attr_map.get("src") or attr_map.get("data-src") or attr_map.get("data-original")
            if src:
                self.images.append(urljoin(self.base_url, src))
            return

        if tag == "title":
            self._capture_title = True
            return

        if tag == "p" and self._blocked_depth == 0:
            self._capture_paragraph = True
            self._current_paragraph = []
            return

        if tag == "br" and self._capture_paragraph:
            self._current_paragraph.append(" ")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"}:
            self._blocked_depth = max(0, self._blocked_depth - 1)
            return

        if tag == "title":
            self._capture_title = False
            return

        if tag == "p" and self._capture_paragraph:
            text = clean_extracted_line("".join(self._current_paragraph))
            if text:
                self.paragraphs.append(text)
            self._capture_paragraph = False
            self._current_paragraph = []

    def handle_data(self, data: str) -> None:
        if self._capture_paragraph and self._blocked_depth == 0:
            self._current_paragraph.append(data)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def create_http_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-IN,en;q=0.9",
        }
    )
    return session


SESSION = create_http_session()


def now_ist() -> datetime:
    return datetime.now(IST)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def clean_headline(title: str) -> str:
    raw = normalize_whitespace(title)
    if " - " in raw:
        head, tail = raw.rsplit(" - ", 1)
        if tail and len(tail.split()) <= 6:
            return head.strip()
    return raw


def strip_html_tags(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    clean = unescape(clean)
    return normalize_whitespace(clean)


def safe_text(text: str, fallback: str) -> str:
    text = normalize_whitespace(text)
    return text if text else fallback


def topic_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


def truncate_text(text: str, limit: int) -> str:
    text = normalize_whitespace(text)
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0].strip()
    return cut + "..."


def ensure_sentence(text: str) -> str:
    text = normalize_whitespace(text)
    if not text:
        return ""
    if text[-1] in ".!?":
        return text
    return text + "."


def count_words(text: str) -> int:
    return len(re.findall(r"\b[\w'-]+\b", text or ""))


def dedupe_strings(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    result: List[str] = []
    for item in items:
        key = normalize_whitespace(item).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(normalize_whitespace(item))
    return result


def trim_to_word_limit(text: str, max_words: int) -> str:
    words = re.findall(r"\S+", text or "")
    if len(words) <= max_words:
        return normalize_whitespace(text)
    trimmed = " ".join(words[:max_words]).rstrip(",;:")
    if trimmed and trimmed[-1] not in ".!?":
        trimmed += "."
    return normalize_whitespace(trimmed)


def fit_paragraph(sentences: Sequence[str], min_words: int, max_words: int) -> str:
    cleaned = [ensure_sentence(sentence) for sentence in dedupe_strings(sentences)]
    paragraph = " ".join(cleaned)
    if count_words(paragraph) < min_words:
        filler = (
            "Readers are therefore watching not only the headline itself, but also the secondary signals "
            "that can confirm direction, execution, and broader consequences."
        )
        while count_words(paragraph) < min_words:
            paragraph = f"{paragraph} {ensure_sentence(filler)}".strip()
    if count_words(paragraph) > max_words:
        paragraph = trim_to_word_limit(paragraph, max_words)
    return paragraph


def clean_extracted_line(text: str) -> str:
    text = strip_html_tags(text)
    text = re.sub(r"\s+\|\s+.*$", "", text)
    if len(text) < 35:
        return ""
    if any(pattern.match(text) for pattern in NOISY_LINE_PATTERNS):
        return ""
    return text


def normalize_media_url(url: str, base_url: str = "") -> str:
    candidate = normalize_whitespace(url)
    if not candidate:
        return ""
    if candidate.startswith("//"):
        candidate = "https:" + candidate
    if base_url:
        candidate = urljoin(base_url, candidate)
    return candidate


def looks_like_valid_image(url: str) -> bool:
    lowered = url.lower()
    return url.startswith(("http://", "https://")) and not any(hint in lowered for hint in IMAGE_BLOCKLIST_HINTS)


def build_search_feed_url(query: str) -> str:
    return (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl=en-IN&gl=IN&ceid=IN:en"
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

    try:
        if token_json:
            return Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
        if token_b64:
            decoded = base64.b64decode(token_b64).decode("utf-8")
            return Credentials.from_authorized_user_info(json.loads(decoded), SCOPES)
    except (ValueError, json.JSONDecodeError) as exc:
        logging.warning("Could not parse Blogger token from environment: %s", exc)

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


def fetch_url_content(url: str, timeout: int) -> bytes:
    response = SESSION.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


def fetch_feed(feed_url: str) -> feedparser.FeedParserDict:
    return feedparser.parse(fetch_url_content(feed_url, REQUEST_TIMEOUT))


def extract_rss_image(entry: feedparser.FeedParserDict) -> str:
    candidates: List[str] = []
    media_content = entry.get("media_content", []) or []
    media_thumbnail = entry.get("media_thumbnail", []) or []
    links = entry.get("links", []) or []

    if isinstance(media_content, dict):
        media_content = [media_content]
    if isinstance(media_thumbnail, dict):
        media_thumbnail = [media_thumbnail]

    for media in list(media_content) + list(media_thumbnail):
        if isinstance(media, dict) and media.get("url"):
            candidates.append(media["url"])

    for link in links:
        if isinstance(link, dict) and str(link.get("type", "")).startswith("image/"):
            candidates.append(link.get("href", ""))

    for candidate in candidates:
        normalized = normalize_media_url(candidate)
        if looks_like_valid_image(normalized):
            return normalized
    return ""


def parse_feed_items(parsed_feed: feedparser.FeedParserDict, category: str) -> List[NewsItem]:
    items: List[NewsItem] = []
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
                rss_image=extract_rss_image(entry),
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


def fetch_category_search_items(category: str, query: str) -> List[NewsItem]:
    parsed_feed = fetch_feed(build_search_feed_url(query))
    return parse_feed_items(parsed_feed, category)


def fetch_trending_news() -> Dict[str, List[NewsItem]]:
    logging.info("Fetching Google News India trending feed")
    primary_feed = fetch_feed(GOOGLE_NEWS_RSS_URL)
    primary_items = parse_feed_items(primary_feed, "General")

    category_map: Dict[str, List[NewsItem]] = {category: [] for category in CATEGORIES}
    for category in CATEGORIES:
        category_map[category].extend(
            [item for item in primary_items if category_score(item, category) > 0]
        )

    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for category, queries in CATEGORY_SEARCH_TERMS.items():
            for query in queries:
                futures[executor.submit(fetch_category_search_items, category, query)] = (category, query)

        for future in as_completed(futures):
            category, query = futures[future]
            try:
                category_map[category].extend(future.result())
            except requests.RequestException as exc:
                logging.warning("Feed request failed for %s (%s): %s", category, query, exc)

    for category in CATEGORIES:
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
    base_title = truncate_text(base_title, 95)
    options = [
        base_title,
        f"{base_title} | {category} News Analysis",
        f"{base_title} | What It Means for India",
        f"{base_title} | {now_ist().strftime('%d %b %Y')}",
    ]

    for option in options:
        key = topic_key(option)
        if key not in used_titles:
            used_titles.add(key)
            return option

    final_title = f"{base_title} | {category} Update {int(time.time())}"
    used_titles.add(topic_key(final_title))
    return final_title


def build_seo_title(item: NewsItem, category: str, used_titles: Set[str]) -> str:
    headline = re.sub(r"\s+", " ", item.headline).strip(" -:")
    if "india" not in headline.lower():
        headline = f"{headline}: What It Means for India"
    if category.lower() not in headline.lower():
        headline = f"{headline} | {category}"
    return unique_title(headline, category, used_titles)


def extract_meta_value(meta_tags: Sequence[Dict[str, str]], names: Sequence[str]) -> str:
    lookup = {name.lower() for name in names}
    for attrs in meta_tags:
        key = (attrs.get("property") or attrs.get("name") or "").strip().lower()
        content = normalize_whitespace(unescape(attrs.get("content", "")))
        if key in lookup and content:
            return content
    return ""


@lru_cache(maxsize=128)
def fetch_source_details(url: str) -> SourceDetails:
    try:
        response = SESSION.get(url, timeout=SOURCE_FETCH_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        logging.warning("Could not fetch source page for %s: %s", url, exc)
        return SourceDetails(description="", snippets=[])

    parser = SourceHTMLParser(response.url or url)
    parser.feed(response.text)
    parser.close()

    description = extract_meta_value(
        parser.meta_tags,
        ("description", "og:description", "twitter:description"),
    )
    snippets: List[str] = []
    for paragraph in parser.paragraphs:
        cleaned = clean_extracted_line(paragraph)
        if cleaned and 40 <= len(cleaned) <= 420:
            snippets.append(cleaned)
    snippets = dedupe_strings(snippets)[:5]

    og_image = normalize_media_url(
        extract_meta_value(parser.meta_tags, ("og:image", "twitter:image", "og:image:url")),
        response.url or url,
    )
    first_image = ""
    for image in parser.images:
        normalized = normalize_media_url(image, response.url or url)
        if looks_like_valid_image(normalized):
            first_image = normalized
            break

    if not description and snippets:
        description = snippets[0]

    return SourceDetails(
        description=description,
        snippets=snippets,
        og_image=og_image,
        first_image=first_image,
    )


def validate_image_url(url: str) -> str:
    if not looks_like_valid_image(url):
        return ""

    try:
        response = SESSION.head(url, allow_redirects=True, timeout=IMAGE_TIMEOUT)
        content_type = response.headers.get("Content-Type", "").lower()
        if response.ok and content_type.startswith("image/"):
            return response.url or url
    except requests.RequestException:
        pass

    try:
        response = SESSION.get(url, stream=True, allow_redirects=True, timeout=IMAGE_TIMEOUT)
        content_type = response.headers.get("Content-Type", "").lower()
        final_url = response.url or url
        response.close()
        if response.ok and content_type.startswith("image/"):
            return final_url
    except requests.RequestException:
        pass

    return ""


def build_unsplash_fallback(category: str, headline: str) -> str:
    query_seed = f"{category.lower()} india news {headline}"
    keywords = re.findall(r"[a-zA-Z]{4,}", query_seed)[:6]
    query = ",".join(keywords) if keywords else f"{category.lower()},india,news"
    return UNSPLASH_FALLBACK_TEMPLATE.format(quote_plus(query))


def resolve_featured_image(item: NewsItem, category: str, source_details: SourceDetails) -> str:
    candidates = [
        normalize_media_url(item.rss_image),
        normalize_media_url(source_details.og_image),
        normalize_media_url(source_details.first_image),
    ]

    for candidate in candidates:
        try:
            validated = validate_image_url(candidate)
            if validated:
                return validated
        except Exception as exc:
            logging.warning("Image validation failed for %s: %s", candidate, exc)

    fallback = build_unsplash_fallback(category, item.headline)
    validated_fallback = validate_image_url(fallback)
    return validated_fallback or fallback


def build_section_context(
    item: NewsItem,
    category: str,
    source_details: SourceDetails,
) -> Dict[str, str]:
    brief = CATEGORY_BRIEF[category]
    description = safe_text(source_details.description, item.summary)
    snippet_one = source_details.snippets[0] if len(source_details.snippets) > 0 else description
    snippet_two = source_details.snippets[1] if len(source_details.snippets) > 1 else snippet_one
    snippet_three = source_details.snippets[2] if len(source_details.snippets) > 2 else snippet_two
    return {
        "headline": truncate_text(item.headline, 170),
        "summary": ensure_sentence(truncate_text(item.summary, 280)),
        "description": ensure_sentence(truncate_text(description, 300)),
        "snippet_one": ensure_sentence(truncate_text(snippet_one, 260)),
        "snippet_two": ensure_sentence(truncate_text(snippet_two, 260)),
        "snippet_three": ensure_sentence(truncate_text(snippet_three, 260)),
        "source": item.source,
        "published": item.published,
        "audience": brief["audience"],
        "impact": brief["impact"],
        "trend": brief["trend"],
        "reaction": brief["reaction"],
        "economy": brief["economy"],
        "experts": brief["experts"],
        "next_steps": brief["next_steps"],
        "category_lower": category.lower(),
    }


def build_article_sections(
    item: NewsItem,
    category: str,
    source_details: SourceDetails,
) -> List[Tuple[str, str]]:
    context = build_section_context(item, category, source_details)
    sections = {
        "Introduction": [
            f"{context['headline']} has become an important {context['category_lower']} story in India after fresh coverage from {context['source']} highlighted a development with direct relevance for {context['audience']}.",
            f"The story is receiving attention because {context['summary']}",
            f"At a time when decision-makers are closely tracking {context['trend']}, even a single high-visibility update can quickly influence expectations about planning, pricing, implementation, or investment choices.",
            "That is why this development has moved beyond a routine headline and become a story with practical value for readers who need a reliable reading of the situation.",
            f"Available source material indicates that {context['description']}",
            "The combination of policy interest, sector impact, and public attention makes it important to separate verified developments from speculation or short-term noise.",
            "This report therefore looks at the background, the latest developments, the early reaction from key stakeholders, and the likely implications for India over the coming days and weeks.",
        ],
        "Background Context": [
            f"To understand the current headline, it helps to place it within the wider pattern of {context['trend']} that has been shaping the Indian landscape.",
            f"In recent months, the broader {context['category_lower']} environment has been influenced by shifting demand, changing expectations, and a stronger focus on execution quality across institutions.",
            "That backdrop matters because new announcements rarely operate in isolation; they usually interact with earlier decisions, unresolved pressures, and structural trends that were already building below the surface.",
            f"Source-linked material suggests the story sits inside that broader setting rather than outside it, with early descriptions noting that {context['snippet_one']}",
            "Readers should therefore treat the latest update as part of an unfolding sequence, not simply as a one-day development without historical or institutional context.",
            f"When the background is considered carefully, the headline begins to reveal why the issue could influence {context['impact']} more meaningfully than the first summary may suggest.",
            "That historical and sector context is essential for understanding whether the current moment represents a temporary adjustment, a policy signal, or the beginning of a more sustained shift.",
        ],
        "Latest Developments": [
            f"The latest developments, as reflected in reporting published around {context['published']}, suggest that the story is moving quickly and remains open to further clarification.",
            f"Current coverage points to a central development: {context['summary']}",
            f"Additional source details indicate that {context['snippet_two']}",
            "This matters because fast-moving stories often reshape expectations in stages, with the first report setting the tone and later disclosures sharpening the consequences.",
            f"The update has also widened the conversation beyond the immediate headline, with readers now examining how the issue may influence near-term priorities across the {context['category_lower']} ecosystem.",
            "In practical terms, the latest phase is less about isolated commentary and more about whether the early signals will be confirmed by official communication, follow-up disclosures, or measurable outcomes.",
            "Until that next layer of confirmation appears, the most credible interpretation is to focus on what has been reported, what remains unclear, and which indicators are likely to settle the debate.",
        ],
        "Government and Market Reaction": [
            f"Early reaction is centered on how {context['reaction']} may interpret the latest headline and adjust their next steps in response.",
            "In government-linked stories, official silence can be as important as formal announcements, while in market-linked stories, pricing and sentiment can shift before the full facts are available.",
            f"That is why the immediate response is being judged not only by what has been said publicly, but also by how institutions are positioning themselves around {context['impact']}.",
            f"Source signals suggest that observers are already reading the development through a wider lens, especially where {context['snippet_three']}",
            "For sectors that depend on regulatory certainty, financing confidence, or predictable operating conditions, even a measured reaction can shape near-term decisions.",
            "The next meaningful reaction may come through official statements, market pricing, employer actions, or company commentary, depending on the nature of the story and the actors most directly involved.",
            "Until then, the most important takeaway is that stakeholder response is now part of the story itself and will influence how the headline evolves in public and professional discussion.",
        ],
        "Why This Matters": [
            f"This story matters because developments in the {context['category_lower']} space often move from headline to real-world consequence more quickly than many readers expect.",
            f"The issue is not just informational; it has the potential to affect {context['impact']} in ways that can alter planning for households, businesses, departments, or investors.",
            "That practical dimension is what separates a significant update from a routine news item with limited follow-through.",
            f"When the underlying trend involves {context['trend']}, decision-makers need to assess not only the direct announcement but also the second-order implications it may trigger.",
            "In many cases, the most important changes arrive after the first report, once institutions translate the headline into budgets, applications, product decisions, or policy execution.",
            "That is why professional readers tend to focus on durability, implementation, and the quality of confirmation rather than reacting only to the first wave of attention.",
            "Seen through that lens, the current development matters because it could become a reference point for future decisions even if the immediate facts remain fluid for a short period.",
        ],
        "Impact on India and the Economy": [
            f"The likely impact on India will depend on how the story influences {context['economy']} over the near to medium term.",
            "Some headlines create direct effects through regulation, funding, or hiring, while others matter because they shape confidence, expectations, and the timing of major decisions.",
            f"In this case, the wider relevance comes from the possibility that the development may influence {context['impact']} across multiple parts of the economy rather than within a single narrow segment.",
            "If the issue leads to stronger execution, clearer guidance, or more predictable conditions, the benefits could extend beyond the first institutions mentioned in the report.",
            "If the story instead introduces uncertainty, delays, or cost pressure, the effects may be felt through investment plans, operating margins, application timelines, or consumer behavior.",
            f"That is why readers should connect the headline to broader questions around {context['economy']} rather than treating it as an isolated event with limited spillover.",
            "For India, the larger significance lies in whether the current update reinforces stability and momentum or adds another layer of caution to an already complex operating environment.",
        ],
        "What Experts Are Saying": [
            f"Early expert discussion around the story is focused less on headline value and more on the deeper issues of {context['experts']}.",
            "Analysts and sector observers typically look for confirmation, execution signals, and measurable evidence before drawing firm conclusions from an emerging development.",
            "That approach is especially important in India, where a single policy note, market signal, or operational disclosure can carry meaning only when placed inside a wider institutional framework.",
            "The strongest early takeaway is that the quality of follow-up data may matter as much as the first announcement itself.",
            "Experts are also likely to compare the latest development with prior patterns, asking whether it reflects a one-off disruption, a structural shift, or a sign of deeper change.",
            "For now, the professional consensus is likely to remain cautious, evidence-driven, and focused on what can be verified in the next round of updates.",
        ],
        "What Happens Next": [
            f"The next phase of the story will likely revolve around {context['next_steps']}.",
            "This is the point at which early assumptions are either reinforced by concrete information or challenged by new disclosures that force a reassessment.",
            "Readers should watch for timelines, implementation details, and secondary indicators that reveal whether the development is gaining institutional backing or losing momentum.",
            "If official communication becomes clearer, the narrative may stabilize quickly and allow markets, applicants, companies, or citizens to plan with more confidence.",
            "If new information raises fresh questions, the story could remain fluid and lead to another round of interpretation before the true impact is visible.",
            "The most sensible approach is to follow credible primary updates, compare them with the initial coverage, and focus on indicators that show real-world execution rather than rhetoric alone.",
        ],
        "Conclusion": [
            f"In conclusion, {context['headline']} is more than a passing {context['category_lower']} headline and deserves close attention from readers across India.",
            "The key reason is that the story touches practical decision-making, broader expectations, and the quality of execution that often defines whether a development becomes meaningful.",
            "While not every early headline leads to lasting change, this one has enough context and enough stakeholder interest to justify continued tracking.",
            "The most important next step is to watch how fresh disclosures, official responses, and measurable outcomes either strengthen or soften the current narrative.",
            f"Until then, the clearest reading is that the story has real relevance for {context['audience']} and may remain an important reference point in the days ahead.",
        ],
    }

    built_sections: List[Tuple[str, str]] = []
    for heading, min_words, max_words in ARTICLE_SECTIONS:
        built_sections.append((heading, fit_paragraph(sections[heading], min_words, max_words)))
    return built_sections


def render_featured_image(image_url: str, title: str) -> str:
    if not image_url:
        return ""
    return (
        '<figure style="margin:0 0 24px 0;">'
        f'<img src="{escape(image_url, quote=True)}" alt="{escape(title)}" '
        'style="display:block;width:100%;max-width:1200px;height:auto;margin:0 auto;" />'
        "</figure>"
    )


def build_article_html(
    item: NewsItem,
    title: str,
    category: str,
    sections: Sequence[Tuple[str, str]],
    featured_image_url: str,
) -> str:
    published_stamp = now_ist().strftime("%d %b %Y, %I:%M %p IST")
    parts: List[str] = []
    image_html = render_featured_image(featured_image_url, title)
    if image_html:
        parts.append(image_html)

    parts.append(f"<p><em>Published: {escape(published_stamp)} | Category: {escape(category)}</em></p>")

    paragraph_count = 0
    for heading, paragraph in sections:
        parts.append(f"<h2>{escape(heading)}</h2>")
        parts.append(f"<p>{escape(paragraph)}</p>")
        paragraph_count += 1
        if paragraph_count == 1:
            parts.append(NATIVE_BANNER_CODE)
        elif paragraph_count > 1 and paragraph_count % 2 == 1:
            parts.append(BANNER_AD_CODE)

    parts.append(
        (
            "<p><strong>Source attribution:</strong> "
            f'<a href="{escape(item.link, quote=True)}" target="_blank" '
            f'rel="noopener noreferrer nofollow">{escape(item.source)}</a></p>'
        )
    )
    return "\n\n".join(parts)


def generate_article(item: NewsItem, category: str, used_titles: Set[str]) -> ArticleDraft:
    source_details = fetch_source_details(item.link)
    title = build_seo_title(item, category, used_titles)
    sections = build_article_sections(item, category, source_details)
    featured_image_url = resolve_featured_image(item, category, source_details)
    html = build_article_html(item, title, category, sections, featured_image_url)
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
            try:
                draft = generate_article(item, category, used_titles)
                response = publish_to_blogger(service, draft)
                log_post_result(response, draft.title)
                state["published_titles"].append(draft.title)
                state["published_topics"].append(draft.topic_key)
                save_state(STATE_FILE, state)
                published_count += 1
                time.sleep(PUBLISH_DELAY_SECONDS)
            except HttpError as exc:
                logging.error("Blogger API error for '%s': %s", item.headline, exc)
            except Exception as exc:
                logging.error("Unexpected publish error for '%s': %s", item.headline, exc)

    logging.info(
        "Automation run complete | attempted=%s | published=%s",
        attempted_count,
        published_count,
    )


if __name__ == "__main__":
    main()
