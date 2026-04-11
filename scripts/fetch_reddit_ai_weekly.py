#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_reddit_ai_weekly.py — Reddit AI週報 完全自動パイプライン
Track B v3 第3号 reddit-ai-weekly

役割 (GitHub Actions上で週次実行):
1. Reddit search.json を 40軸 (10 keywords × 2 time_windows × 2 sorts) でクロス検索
2. cross_hit_count ≥ 2 でフィルタ → score 降順で top 40 候補を選定
3. 各候補の permalink.json から上位5コメントを取得
4. Claude Sonnet 4.5 に全候補を渡して「pillars 3 + buzz 10 + real_opinion 5」の構造化JSON を1回で生成
5. 出力: data/reddit-ai-weekly/weekly-YYYY-MM-DD.json + latest.json

使用方法:
  通常実行（当週分・月曜実行想定）:
    python fetch_reddit_ai_weekly.py

環境変数:
  ANTHROPIC_API_KEY — Claude Sonnet 4.5 APIキー

lastVerified: 2026-04-11
"""

import os
import sys
import json
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    import anthropic
except ImportError:
    print("[ERROR] anthropic パッケージが必要です: pip install anthropic")
    sys.exit(1)

# ====================================================
# 定数
# ====================================================
USER_AGENT = "AIJapanIndex/1.0 (by /u/taiga_beep) weekly research tool"

KEYWORDS = ["AI", "LLM", "Claude", "ChatGPT", "Gemini", "OpenAI",
            "Anthropic", "LocalLLaMA", "GPT", "Copilot"]
TIME_WINDOWS = ["day", "week"]
SORT_MODES = ["top", "comments"]

CANDIDATE_TOP_N = 40       # Sonnetに渡す候補数
COMMENTS_PER_POST = 5      # 各候補から取るコメント数
SELFTEXT_MAX = 500         # Sonnetに渡す selftext 最大長
COMMENT_MAX = 400          # Sonnetに渡すコメント本文最大長

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "reddit-ai-weekly"

CLAUDE_MODEL = "claude-sonnet-4-5-20250929"

# ====================================================
# Reddit fetch (40軸クロス検索)
# ====================================================

def http_get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"  [WARN] HTTP {e.code}: {url}")
        return None
    except Exception as e:
        print(f"  [WARN] {e}: {url}")
        return None


def extract_posts_from_listing(data, source_tag):
    if not data or "data" not in data:
        return []
    children = data.get("data", {}).get("children", [])
    out = []
    for c in children:
        d = c.get("data", {})
        if d.get("over_18"):  # NSFW除外
            continue
        out.append({
            "id": d.get("id"),
            "subreddit": d.get("subreddit"),
            "title": d.get("title"),
            "selftext": (d.get("selftext") or "")[:3000],
            "url": d.get("url"),
            "permalink": "https://www.reddit.com" + d.get("permalink", ""),
            "author": d.get("author"),
            "score": d.get("score", 0),
            "num_comments": d.get("num_comments", 0),
            "created_utc": d.get("created_utc"),
            "source_axes": [source_tag],
        })
    return out


def fetch_reddit_40axes():
    """40軸クロス検索 → 重複マージ → cross_hit_count 計算"""
    print("[PHASE 1/4] Reddit 40軸クロス検索")
    seen = {}  # id -> post

    for kw in KEYWORDS:
        for t in TIME_WINDOWS:
            for sort in SORT_MODES:
                tag = f"{kw}/{t}/{sort}"
                url = f"https://www.reddit.com/search.json?q={kw}&sort={sort}&t={t}&limit=50"
                data = http_get_json(url)
                if not data:
                    time.sleep(1.2)
                    continue
                posts = extract_posts_from_listing(data, tag)
                for p in posts:
                    if not p.get("id"):
                        continue
                    if p["id"] in seen:
                        seen[p["id"]]["source_axes"].append(tag)
                    else:
                        seen[p["id"]] = p
                time.sleep(0.6)  # Reddit rate limit

    for pid, p in seen.items():
        p["cross_hit_count"] = len(set(p["source_axes"]))

    posts = list(seen.values())
    print(f"  → unique posts: {len(posts)}")
    return posts


# ====================================================
# 候補選定
# ====================================================

def select_candidates(posts, top_n=CANDIDATE_TOP_N):
    print(f"[PHASE 2/4] 候補選定 (cross_hit ≥ 2 優先, top {top_n})")
    cross = [p for p in posts if p.get("cross_hit_count", 1) >= 2]
    cross.sort(key=lambda p: (p.get("cross_hit_count", 0), p.get("score", 0)), reverse=True)

    singles = [p for p in posts if p.get("cross_hit_count", 1) < 2]
    singles.sort(key=lambda p: p.get("score", 0), reverse=True)

    selected = cross[:top_n]
    if len(selected) < top_n:
        selected += singles[: top_n - len(selected)]
    print(f"  → selected: {len(selected)}")
    return selected


# ====================================================
# コメント取得
# ====================================================

def fetch_top_comments(permalink, top_k=COMMENTS_PER_POST):
    url = permalink.rstrip("/") + ".json?sort=top&limit=20"
    data = http_get_json(url)
    if not isinstance(data, list) or len(data) < 2:
        return []
    children = data[1].get("data", {}).get("children", [])
    comments = []
    for c in children:
        if c.get("kind") != "t1":
            continue
        d = c.get("data", {})
        if d.get("stickied"):
            continue
        body = d.get("body") or ""
        if not body or body in ("[removed]", "[deleted]"):
            continue
        comments.append({
            "author": d.get("author"),
            "body": body[:2000],
            "score": d.get("score", 0),
            "is_submitter": d.get("is_submitter", False),
        })
    comments.sort(key=lambda c: c.get("score", 0), reverse=True)
    return comments[:top_k]


def enrich_with_comments(candidates):
    print(f"[PHASE 3/4] コメント取得 ({len(candidates)} posts × {COMMENTS_PER_POST})")
    enriched = []
    for i, p in enumerate(candidates, 1):
        permalink = p.get("permalink", "")
        cm = fetch_top_comments(permalink)
        p["top_comments"] = cm
        enriched.append(p)
        if i % 10 == 0:
            print(f"  [{i}/{len(candidates)}] fetched")
        time.sleep(0.6)
    return enriched


# ====================================================
# Claude Sonnet 4.5 deep-read (単発1コール)
# ====================================================

PROMPT_SYSTEM = """あなたはAI Japan Indexの週次リサーチアナリスト。
戦場は日本AIコミュニティ。読者は日本でAIを実装・活用している実務者・経営層・初学者。
情報源は海外Redditだが、出力は必ず日本語で日本の文脈に翻訳する。

あなたの仕事:
入力された Reddit 上位投稿データ (title/selftext/top_comments) を深く読み、
週刊レポート「Reddit AI週報」の構造化JSON を1回の応答で返す。"""


def build_deep_read_prompt(candidates):
    """候補データをコンパクトなブロックに整形"""
    blocks = []
    for i, p in enumerate(candidates, 1):
        title = (p.get("title") or "").strip()
        selftext = (p.get("selftext") or "").strip()[:SELFTEXT_MAX]
        sub = p.get("subreddit", "")
        score = p.get("score", 0)
        ncom = p.get("num_comments", 0)
        cross = p.get("cross_hit_count", 1)
        url = p.get("permalink", "")
        ext_url = p.get("url", "") or ""
        if ext_url == url:
            ext_url = ""

        lines = [
            f"### Candidate {i}",
            f"id: {p.get('id')}",
            f"sub: r/{sub}",
            f"score: {score} / comments: {ncom} / cross_hit: {cross}",
            f"url_reddit: {url}",
        ]
        if ext_url:
            lines.append(f"url_external: {ext_url}")
        lines.append(f"title: {title}")
        if selftext:
            lines.append(f"selftext: {selftext}")

        cms = p.get("top_comments") or []
        for j, c in enumerate(cms, 1):
            body = (c.get("body") or "").replace("\n", " ")[:COMMENT_MAX]
            lines.append(f"comment{j} [{c.get('score',0)}up @{c.get('author','')}]: {body}")
        blocks.append("\n".join(lines))

    all_blocks = "\n\n".join(blocks)

    instructions = """# 絶対ルール

## 構造 (この形で JSON を返す)
以下の**正確なキー名・構造**で返す。余計なキーを追加しない:

```
{
  "week_topic_map": {
    "note": "今週話題の大きな柱3本 (各テーマ=複数スレの集合体)",
    "pillars": [
      {
        "theme": "テーマ名 — サブタイトル",
        "heat": "本日最大級 / 上昇中 / くすぶり中 のいずれか",
        "evidence": "根拠となる具体的スレッド (r/xxx タイトル score/コメント) を2-3件連ねる。300-500字",
        "mizuoka_note": "AI Japan Indexの見解。中学生でも『へーなるほど』と思える日本語で200-350字。専門用語は括弧で補足。一人称『私』は使わない"
      }
      // 3本
    ]
  },
  "buzz_layer": [
    {
      "rank": 1,
      "title_original": "英語原題",
      "title_ja": "日本語タイトル 40字程度",
      "url": "permalink",
      "external_url": "外部URL or 空文字",
      "source": "r/xxx",
      "score": 数値,
      "num_comments": 数値,
      "cross_hit_count": 数値,
      "ai_topic_center": "このスレッドのAI関連トピック中心 15-25字",
      "summary_ja": "スレ内容の日本語サマリ 80-180字",
      "quote_original": "印象的な英語引用 200字以内",
      "quote_ja": "上記引用の日本語訳 120字以内",
      "top_comments_ja": [
        {"score": 数値, "body_excerpt_ja": "日本語訳したコメント本文 100-180字"}
      ],
      "mizuoka_note": "AI Japan Indexの見解 130-220字。『へーなるほど』レベル。一人称『私』は使わない"
    }
    // 10件
  ],
  "real_opinion_layer": [
    {
      "rank": 1,
      "title_original": "英語原題",
      "title_ja": "日本語タイトル",
      "url": "permalink",
      "source": "r/xxx",
      "score": 数値,
      "num_comments": 数値,
      "cross_hit_count": 数値,
      "comments_per_score_ratio": 数値 (小数3桁),
      "ai_topic_center": "このスレのトピック中心",
      "summary_ja": "同上",
      "quote_original": "同上",
      "quote_ja": "同上",
      "top_comments_ja": [同上 最大4件],
      "mizuoka_note": "同上"
    }
    // 5件
  ]
}
```

## 選定ロジック (厳守)

- **buzz_layer (10件)**: AIセントラリティ × score で「拡散の面積」が大きいもの TOP10
- **real_opinion_layer (5件)**: comments/score 比率が高く「本音の議論」が起きているもの TOP5 (buzzと重複しても良い)
- **pillars (3本)**: 上記15スレを横断して見える「今週の論点3つ」を構造化

## 文体ルール

- 煽り・情報商材的表現禁止 (「衝撃」「必見」「絶対」禁止)
- 一人称「私」は使用禁止
- 推測禁止。データにない情報を補わない
- 英語技術用語は原語ママ+括弧日本語補足 (例: RAG=検索拡張生成)
- mizuoka_note は「へーなるほど」レベル (中学生でも意味が取れる)。専門用語は必ず括弧で言い換える
- 数字は必ず記事に書く (score / comments / x人 等)

## JSON 出力形式

- 必ず上記スキーマの**JSONのみ**を返す
- マークダウンコードブロック ``` で囲まない
- 先頭に説明文・後置きコメント禁止
- 文字列内の改行は \\n エスケープ
"""

    return instructions + "\n\n# 入力候補データ\n\n" + all_blocks


def call_claude_deep_read(client, candidates):
    print(f"[PHASE 4/4] Claude Sonnet 4.5 deep-read (input candidates={len(candidates)})")
    prompt = build_deep_read_prompt(candidates)

    # プロンプトサイズ表示
    prompt_chars = len(prompt)
    print(f"  prompt size: {prompt_chars:,} chars (~{prompt_chars//4:,} tokens approx)")

    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=16000,
        system=PROMPT_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.content[0].text.strip()

    # コードブロック除去 (念のため)
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].startswith("```") else "\n".join(lines[1:])
    if text.startswith("json"):
        text = text[4:].strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  [ERROR] JSON parse failed: {e}")
        debug_path = OUTPUT_DIR / f"_debug_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        debug_path.write_text(text, encoding="utf-8")
        print(f"  raw output saved to: {debug_path}")
        raise

    print(f"  → parsed: {len(parsed.get('buzz_layer',[]))} buzz / "
          f"{len(parsed.get('real_opinion_layer',[]))} opinion / "
          f"{len(parsed.get('week_topic_map',{}).get('pillars',[]))} pillars")
    return parsed


# ====================================================
# 出力
# ====================================================

def build_final_payload(deep_read_result, raw_post_count, candidate_count, comment_count):
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    week_str = now.strftime("%Y-%m-%d")

    methodology = {
        "fetch": f"Reddit search.json 40軸 (10 keywords × 2 time_windows × 2 sorts) {raw_post_count} unique posts",
        "candidate_selection": f"cross_hit_count 2+ 優先 → score 降順で top {candidate_count} に絞る",
        "comment_fetch": f"各投稿 permalink.json から上位 {COMMENTS_PER_POST} コメント取得 ({comment_count} コメント)",
        "deep_read": "Claude Sonnet 4.5 が selftext + top_comments を深読して構造化 (GitHub Actions 自動実行)",
        "scoring_layers": {
            "buzz_layer": "AIセントラリティ × score (拡散力=最大=拡散の面積)",
            "real_opinion_layer": "comments/score ratio + 議論の実質 (コメントが長く熱い=本音の層)",
        },
        "honesty_note": "本稿は selftext + top_comments の実読データのみで構成。タイトルや score だけから推測した要約は含まない",
    }

    payload = {
        "tool_name_ja": "Reddit AI週報",
        "tool_name_en": "Reddit AI Weekly",
        "tool_id": "reddit-ai-weekly",
        "tagline": "Redditで今週AIについて何が話されたか、本音まで読む週刊レポート",
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%S+09:00"),
        "generated_by": f"Claude Sonnet 4.5 ({CLAUDE_MODEL}) via GitHub Actions",
        "source_type": "reddit",
        "methodology": methodology,
    }
    # deep_read_result の week_topic_map / buzz_layer / real_opinion_layer をマージ
    wtm = deep_read_result.get("week_topic_map", {})
    wtm.setdefault("updated_at", now.strftime("%Y-%m-%d"))
    payload["week_topic_map"] = wtm
    payload["buzz_layer"] = deep_read_result.get("buzz_layer", [])
    payload["real_opinion_layer"] = deep_read_result.get("real_opinion_layer", [])
    return payload, week_str


def save_output(payload, week_str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    weekly_path = OUTPUT_DIR / f"weekly-{week_str}.json"
    latest_path = OUTPUT_DIR / "latest.json"
    with open(weekly_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\n[SAVE] {weekly_path}")
    print(f"[SAVE] {latest_path}")


# ====================================================
# メイン
# ====================================================

def main():
    parser = argparse.ArgumentParser(description="Reddit AI週報 — 完全自動パイプライン")
    parser.add_argument("--dry-run", action="store_true", help="Reddit取得+候補選定のみ (Claude呼ばない)")
    args = parser.parse_args()

    # API key (.env or env var)
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        env_path = Path(__file__).parent.parent.parent.parent / ".env"
        if env_path.exists():
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("ANTHROPIC_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    if not api_key and not args.dry_run:
        print("[ERROR] ANTHROPIC_API_KEY が必要です")
        sys.exit(1)

    posts = fetch_reddit_40axes()
    raw_count = len(posts)
    candidates = select_candidates(posts)
    enriched = enrich_with_comments(candidates)
    comment_count = sum(len(p.get("top_comments", [])) for p in enriched)

    if args.dry_run:
        print("\n[DRY RUN] 候補選定まで完了。Claudeコールはスキップ")
        return 0

    client = anthropic.Anthropic(api_key=api_key)
    deep_read = call_claude_deep_read(client, enriched)

    payload, week_str = build_final_payload(deep_read, raw_count, len(candidates), comment_count)
    save_output(payload, week_str)
    print("\n[DONE] Reddit AI週報 latest.json 更新完了")
    return 0


if __name__ == "__main__":
    sys.exit(main())
