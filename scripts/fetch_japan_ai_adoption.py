"""
fetch_japan_ai_adoption.py (aji-data- repo 用)
日本企業 AI 導入事例 日次トラッカー — データ取得パイプライン

Project_00/projects/ai-japan-index/app/japan-ai-adoption-daily/scripts/fetch_daily.py を
aji-data- repo 内で動作する形に移植したもの。

書き込み先: aji-data-/data/japan-ai-adoption-daily/latest.json
             aji-data-/data/japan-ai-adoption-daily/archive/YYYY-MM-DD.json
"""

import os
import sys
import json
import hashlib
import logging
import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import feedparser
import anthropic
from dateutil import parser as dateutil_parser

# ─── 定数 ────────────────────────────────────────────────────────────────────

JST = timezone(timedelta(hours=9))

USER_AGENT = "japan-ai-adoption-tracker/1.0 (+https://ai-japan-index.com/)"

RSS_SOURCES = [
    {"name": "ITmedia AI+",         "url": "https://rss.itmedia.co.jp/rss/2.0/aiplus.xml",     "format": "rss2"},
    {"name": "ITmedia Enterprise",  "url": "https://rss.itmedia.co.jp/rss/2.0/enterprise.xml", "format": "rss2"},
    {"name": "Nikkei xTECH IT",     "url": "https://xtech.nikkei.com/rss/xtech-it.rdf",        "format": "rdf"},
    {"name": "PR TIMES",            "url": "https://prtimes.jp/index.rdf",                     "format": "rdf", "has_dc_corp": True},
    {"name": "@Press",              "url": "https://www.atpress.ne.jp/rss/index.rdf",          "format": "rss2"},
    {"name": "ASCII.jp",            "url": "https://ascii.jp/rss.xml",                         "format": "rss2"},
]

KEYWORDS_PRIMARY = [
    "AI", "LLM", "生成AI", "生成的AI",
    "ChatGPT", "Claude", "Gemini", "Copilot", "GPT", "DALL-E",
    "Llama", "Mistral", "Sakana", "推論モデル",
    "LangChain", "RAG", "MCP", "エージェント", "agentic",
    "機械学習", "深層学習", "基盤モデル", "foundation model",
    "AI導入", "AI活用", "AI採用", "AI展開", "AI提供",
    "AI実装", "AI運用", "AI稼働",
]

INDUSTRIES = [
    "製造業", "金融・保険", "小売・EC", "IT・通信・ソフトウェア",
    "広告・メディア・エンタメ", "建設・不動産", "運輸・物流",
    "医療・ヘルスケア", "教育・人材", "官公庁・自治体",
    "士業・コンサルティング", "その他サービス業",
]

PURPOSE_CATEGORIES = [
    "カスタマーサポート", "文書作成", "コード生成", "データ分析",
    "社内検索", "業務自動化", "マーケ支援", "人事評価", "意思決定支援",
]

MODEL_ID = "claude-haiku-4-5-20251001"

# ─── ロガー ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── パス設定 (aji-data- repo 用) ────────────────────────────────────────────

# scripts/fetch_japan_ai_adoption.py から見た repo root
REPO_ROOT = Path(__file__).parent.parent.resolve()
DATA_DIR = REPO_ROOT / "data" / "japan-ai-adoption-daily"
ARCHIVE_DIR = DATA_DIR / "archive"
LATEST_JSON = DATA_DIR / "latest.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# ─── A: RSS 取得 ──────────────────────────────────────────────────────────────

def fetch_rss(source: dict) -> list[dict]:
    name = source["name"]
    url = source["url"]
    try:
        feed = feedparser.parse(url, agent=USER_AGENT, request_headers={"User-Agent": USER_AGENT})
        if feed.bozo:
            logger.warning("%s: bozo flag: %s", name, feed.bozo_exception)
        articles = []
        for entry in feed.entries:
            link = entry.get("link", "")
            if not link:
                continue
            title = entry.get("title", "")
            summary = entry.get("summary", "") or entry.get("description", "")
            dc_corp = entry.get("dc_corp", "") or entry.get("tags_term", "")
            pub_date = _parse_date(entry)
            articles.append({
                "title": title.strip(),
                "description": summary.strip()[:500],
                "source_url": link.strip(),
                "source_name": name,
                "dc_corp": dc_corp.strip() if dc_corp else "",
                "pub_date": pub_date,
            })
        logger.info("%s: %d 件取得", name, len(articles))
        return articles
    except Exception as e:
        logger.error("%s: RSS 取得失敗 — %s", name, e)
        return []


def _parse_date(entry) -> str:
    if entry.get("published_parsed"):
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.astimezone(JST).isoformat()
        except Exception:
            pass
    for key in ("dc_date", "updated"):
        val = entry.get(key, "")
        if val:
            try:
                return dateutil_parser.parse(val).astimezone(JST).isoformat()
            except Exception:
                pass
    return datetime.now(JST).date().isoformat()


def fetch_all_rss() -> list[dict]:
    all_articles = []
    for source in RSS_SOURCES:
        articles = fetch_rss(source)
        all_articles.extend(articles)
    logger.info("合計取得: %d 件", len(all_articles))
    return all_articles

# ─── B: URL 正規化 + 重複排除 ─────────────────────────────────────────────────

def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = parsed.path.rstrip("/") + "/"
        return urlunparse((parsed.scheme, netloc, path, "", "", ""))
    except Exception:
        return url


def url_hash(url: str) -> str:
    return hashlib.md5(normalize_url(url).encode()).hexdigest()


def deduplicate_by_url(articles: list[dict], existing_hashes: set[str]) -> list[dict]:
    seen = set(existing_hashes)
    deduped = []
    for a in articles:
        h = url_hash(a["source_url"])
        if h not in seen:
            seen.add(h)
            a["hash"] = h
            deduped.append(a)
    logger.info("URL 重複排除後: %d 件 (元: %d 件)", len(deduped), len(articles))
    return deduped


def group_same_events(articles: list[dict]) -> list[dict]:
    corp_date_map: dict[str, list[int]] = {}
    for i, a in enumerate(articles):
        corp = a.get("dc_corp", "").strip()
        if not corp:
            continue
        pub = a.get("pub_date", "")[:10]
        key = f"{corp}::{pub}"
        corp_date_map.setdefault(key, []).append(i)

    processed = set()
    for _, indices in corp_date_map.items():
        if len(indices) <= 1:
            continue
        primary_idx = indices[0]
        for dup_idx in indices[1:]:
            articles[dup_idx]["same_event_primary"] = articles[primary_idx]["hash"]
            processed.add(dup_idx)

    result = [a for i, a in enumerate(articles) if i not in processed]
    logger.info("イベントグルーピング後: %d 件", len(result))
    return result

# ─── C: 1次キーワードフィルタ ────────────────────────────────────────────────

def keyword_filter(articles: list[dict]) -> list[dict]:
    filtered = []
    for a in articles:
        text = (a.get("title", "") + " " + a.get("description", "") + " " + a.get("dc_corp", "")).lower()
        if any(kw.lower() in text for kw in KEYWORDS_PRIMARY):
            filtered.append(a)
    logger.info("1次キーワードフィルタ後: %d 件", len(filtered))
    return filtered

# ─── D: Claude Haiku 4.5 判定 + 構造化抽出 ───────────────────────────────────

SYSTEM_PROMPT = f"""あなたは日本企業のAI導入事例を判定・分類する専門家です。

## タスク
与えられた記事情報が「日本企業による実際のAI導入事例」かどうかを判定し、
導入事例であれば構造化情報を抽出してください。

## 判定基準
- YES (is_adoption: true): 日本企業が自社業務にAIを実際に導入・活用した事例
- NO (is_adoption: false): AIベンダー自身の製品リリース / 業界解説 / 海外企業のみ / 検討中のみ
- NULL (is_adoption: null): 判定が困難な場合

## 業種分類 (12分類)
{chr(10).join(f"  - {ind}" for ind in INDUSTRIES)}

## 導入目的分類 (9分類)
{chr(10).join(f"  - {cat}" for cat in PURPOSE_CATEGORIES)}

## 出力形式
必ず以下の JSON のみを返してください。
{{
  "is_adoption": true または false または null,
  "company": "導入企業名（日本語。不明なら null）",
  "industry": "業種分類（上記12分類のいずれか。不明なら null）",
  "vendor": "AIベンダー名。内製なら '内製'。不明なら null",
  "tool": "AIツール・サービス名。不明なら null",
  "purpose_category": "導入目的分類（上記9分類のいずれか。不明なら null）",
  "confidence": 0.0〜1.0の信頼度スコア
}}"""


def classify_articles(articles: list[dict], client: anthropic.Anthropic, dry_run: bool = False) -> list[dict]:
    results = []
    total = len(articles)
    logger.info("LLM 判定開始: %d 件", total)

    for i, article in enumerate(articles):
        if dry_run and i >= 5:
            article.update({
                "is_adoption": None, "company": None, "industry": None,
                "vendor": None, "tool": None, "purpose_category": None,
                "confidence": 0.0, "_dry_run_skip": True,
            })
            results.append(article)
            continue

        user_content = f"""タイトル: {article.get('title', '')}
説明: {article.get('description', '')[:300]}
企業名(PR TIMES dc:corp): {article.get('dc_corp', '') or '不明'}
媒体: {article.get('source_name', '')}
公開日: {article.get('pub_date', '')[:10]}"""

        retry_count = 0
        max_retries = 3
        raw = ""
        while retry_count < max_retries:
            try:
                response = client.messages.create(
                    model=MODEL_ID,
                    max_tokens=256,
                    system=[{
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": user_content}],
                )
                raw = response.content[0].text.strip()
                if "```" in raw:
                    raw = raw.split("```")[1].replace("json", "").strip()
                classified = json.loads(raw)
                article.update(classified)
                results.append(article)
                break
            except json.JSONDecodeError as e:
                logger.warning("JSON 解析失敗 (件 %d): %s — raw: %s", i, e, raw[:100])
                article.update({"is_adoption": None, "company": None, "industry": None,
                                "vendor": None, "tool": None, "purpose_category": None, "confidence": 0.0})
                results.append(article)
                break
            except anthropic.APIStatusError as e:
                retry_count += 1
                logger.warning("Anthropic API エラー (件 %d, retry %d/%d): %s", i, retry_count, max_retries, e)
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                else:
                    article.update({"is_adoption": None, "company": None, "industry": None,
                                    "vendor": None, "tool": None, "purpose_category": None, "confidence": 0.0})
                    results.append(article)
            except Exception as e:
                logger.error("予期しないエラー (件 %d): %s", i, e)
                article.update({"is_adoption": None, "company": None, "industry": None,
                                "vendor": None, "tool": None, "purpose_category": None, "confidence": 0.0})
                results.append(article)
                break

        if not dry_run:
            time.sleep(0.3)

        if (i + 1) % 10 == 0:
            logger.info("  進捗: %d / %d 件完了", i + 1, total)

    adoption_true = sum(1 for r in results if r.get("is_adoption") is True)
    adoption_false = sum(1 for r in results if r.get("is_adoption") is False)
    adoption_null = sum(1 for r in results if r.get("is_adoption") is None)
    logger.info("LLM 判定完了: true=%d / false=%d / null=%d", adoption_true, adoption_false, adoption_null)

    return results

# ─── E+F: JSON 出力 ───────────────────────────────────────────────────────────

def save_results(all_classified: list[dict], today_str: str, existing_latest: list[dict]):
    today_adoptions = [a for a in all_classified if a.get("is_adoption") is True]

    def format_article(a: dict) -> dict:
        return {
            "hash": a.get("hash", ""),
            "title": a.get("title", ""),
            "description": a.get("description", ""),
            "source_url": a.get("source_url", ""),
            "source_name": a.get("source_name", ""),
            "pub_date": a.get("pub_date", ""),
            "company": a.get("company"),
            "industry": a.get("industry"),
            "vendor": a.get("vendor"),
            "tool": a.get("tool"),
            "purpose_category": a.get("purpose_category"),
            "confidence": a.get("confidence", 0.0),
            "is_adoption": a.get("is_adoption"),
            "dc_corp": a.get("dc_corp", ""),
        }

    formatted_today = [format_article(a) for a in today_adoptions]
    pending = [format_article(a) for a in all_classified
               if a.get("is_adoption") is None and not a.get("_dry_run_skip")]

    archive_path = ARCHIVE_DIR / f"{today_str}.json"
    archive_data = {
        "date": today_str,
        "lastUpdated": datetime.now(JST).isoformat(),
        "total_fetched": len(all_classified),
        "total_adoption": len(formatted_today),
        "total_pending": len(pending),
        "articles": formatted_today,
        "pending": pending,
    }
    archive_path.write_text(json.dumps(archive_data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("archive 保存: %s (%d 件)", archive_path, len(formatted_today))

    cutoff_date = (datetime.now(JST).date() - timedelta(days=30)).isoformat()
    merged = existing_latest + formatted_today

    seen_hashes = set()
    unique_merged = []
    for a in merged:
        h = a.get("hash", "")
        if h and h not in seen_hashes:
            seen_hashes.add(h)
            unique_merged.append(a)

    filtered_merged = [a for a in unique_merged if a.get("pub_date", "")[:10] >= cutoff_date]
    filtered_merged.sort(key=lambda a: a.get("pub_date", ""), reverse=True)

    latest_data = {
        "lastUpdated": datetime.now(JST).isoformat(),
        "lastVerified": today_str,
        "total": len(filtered_merged),
        "articles": filtered_merged,
    }
    LATEST_JSON.write_text(json.dumps(latest_data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("latest.json 更新: %d 件 (30日分)", len(filtered_merged))

    return formatted_today, pending


def load_existing_latest() -> list[dict]:
    if not LATEST_JSON.exists():
        return []
    try:
        data = json.loads(LATEST_JSON.read_text(encoding="utf-8"))
        return data.get("articles", [])
    except Exception as e:
        logger.warning("latest.json 読み込み失敗: %s", e)
        return []

# ─── メインエントリ ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="japan-ai-adoption-daily fetch pipeline (aji-data- repo)")
    parser.add_argument("--dry-run", action="store_true",
                        help="RSS 取得まで行い、LLM 判定は最大 5 件のみ")
    args = parser.parse_args()

    dry_run = args.dry_run
    if dry_run:
        logger.info("=== DRY-RUN モード: LLM 判定は最大 5 件のみ ===")

    today_str = datetime.now(JST).date().isoformat()
    logger.info("実行日 (JST): %s", today_str)
    logger.info("書き込み先: %s", DATA_DIR)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY が設定されていません。終了します。")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    existing_latest = load_existing_latest()
    existing_hashes = {a.get("hash", "") for a in existing_latest if a.get("hash")}
    logger.info("既存 latest.json: %d 件", len(existing_latest))

    raw_articles = fetch_all_rss()
    deduped = deduplicate_by_url(raw_articles, existing_hashes)
    grouped = group_same_events(deduped)
    candidates = keyword_filter(grouped)

    if not candidates:
        logger.info("フィルタ後に候補がありませんでした。lastUpdated のみ更新。")
        if existing_latest:
            latest_data = {
                "lastUpdated": datetime.now(JST).isoformat(),
                "lastVerified": today_str,
                "total": len(existing_latest),
                "articles": existing_latest,
            }
            LATEST_JSON.write_text(json.dumps(latest_data, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    classified = classify_articles(candidates, client, dry_run=dry_run)
    today_adoptions, pending = save_results(classified, today_str, existing_latest)

    logger.info("=== 完了 ===")
    logger.info("  本日取得 (is_adoption=true): %d 件", len(today_adoptions))
    logger.info("  保留 (is_adoption=null):    %d 件", len(pending))


if __name__ == "__main__":
    main()
