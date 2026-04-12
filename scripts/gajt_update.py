#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gajt_update.py — GitHub Actions用 GAJT統合更新スクリプト
========================================================
毎日 07:30 UTC (= 16:30 JST) に GitHub Actions から実行される。

処理フロー:
1. Remote OK API + We Work Remotely RSS から求人取得
2. 8社 ATS API (Greenhouse/Lever/Ashby) から求人取得
3. スキル抽出・集計
4. スナップショット保存 (data/gajt/snapshots/YYYY-MM-DD.json)
5. summary.json 生成 (data/gajt/summary.json) → GitHub Pages で公開

出力: data/gajt/summary.json
lastVerified: 2026-04-10
"""

import json
import os
import re
import sys
import time
import datetime
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import timezone, timedelta

# ---------------------------------------------------------------------------
# パス定数 (GitHub Actions では CWD = リポジトリルート)
# ---------------------------------------------------------------------------
BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR       = os.path.join(BASE_DIR, 'data', 'gajt')
SNAPSHOTS_DIR  = os.path.join(DATA_DIR, 'snapshots')
COMP_SNAP_DIR  = os.path.join(DATA_DIR, 'company_snapshots')
SUMMARY_FILE   = os.path.join(DATA_DIR, 'summary.json')
SKILL_HISTORY_FILE = os.path.join(DATA_DIR, 'skill_counts_history.json')  # Ver 2.2: 日付履歴ベース prev_counts
SKILL_DICT_FILE = os.path.join(BASE_DIR, 'scripts', 'skill_dict.json')

for d in [DATA_DIR, SNAPSHOTS_DIR, COMP_SNAP_DIR]:
    os.makedirs(d, exist_ok=True)

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------
HEADERS    = {'User-Agent': 'AJI-Bot/1.0 (+https://ai-japan-index.com/)'}
RETRY_MAX  = 3
RETRY_WAIT = 5

REMOTE_OK_URL = 'https://remoteok.com/api'
WWR_RSS_URL   = 'https://weworkremotely.com/categories/remote-programming-jobs.rss'

# Ver2追加スキル (skill_dict.json に未登録の場合に追加)
EXTRA_SKILLS = ['MCP', 'Agentic RAG', 'vLLM', 'Ray', 'Triton', 'JAX', 'Mojo']

COMPANIES = [
    {
        'slug':       'anthropic',
        'name':       'Anthropic',
        'atsType':    'greenhouse',
        'apiUrl':     'https://boards-api.greenhouse.io/v1/boards/anthropic/jobs?content=true',
        'careersUrl': 'https://www.anthropic.com/jobs',
    },
    {
        'slug':       'openai',
        'name':       'OpenAI',
        'atsType':    'ashby',
        'apiUrl':     'https://api.ashbyhq.com/posting-api/job-board/openai',
        'careersUrl': 'https://openai.com/careers',
    },
    {
        'slug':       'xai',
        'name':       'xAI',
        'atsType':    'greenhouse',
        'apiUrl':     'https://boards-api.greenhouse.io/v1/boards/xai/jobs?content=true',
        'careersUrl': 'https://x.ai/careers',
    },
    {
        'slug':       'mistral',
        'name':       'Mistral AI',
        'atsType':    'lever',
        'apiUrl':     'https://api.lever.co/v0/postings/mistral?mode=json',
        'careersUrl': 'https://mistral.ai/careers',
    },
    {
        'slug':       'cohere',
        'name':       'Cohere',
        'atsType':    'ashby',
        'apiUrl':     'https://api.ashbyhq.com/posting-api/job-board/cohere',
        'careersUrl': 'https://cohere.com/careers',
    },
    {
        'slug':       'perplexity',
        'name':       'Perplexity',
        'atsType':    'ashby',
        'apiUrl':     'https://api.ashbyhq.com/posting-api/job-board/perplexity',
        'careersUrl': 'https://www.perplexity.ai/careers',
    },
    {
        'slug':       'runway',
        'name':       'Runway',
        'atsType':    'ashby',
        'apiUrl':     'https://api.ashbyhq.com/posting-api/job-board/runway',
        'careersUrl': 'https://runwayml.com/careers',
    },
    {
        'slug':       'elevenlabs',
        'name':       'ElevenLabs',
        'atsType':    'ashby',
        'apiUrl':     'https://api.ashbyhq.com/posting-api/job-board/elevenlabs',
        'careersUrl': 'https://elevenlabs.io/careers',
    },
]

# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------
def fetch_json(url, retries=RETRY_MAX):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
                return json.loads(raw)
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            print(f'  [WARN] {url} attempt {attempt+1}/{retries}: {e}', file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(RETRY_WAIT)
    return None


def fetch_xml(url, retries=RETRY_MAX):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()
        except urllib.error.URLError as e:
            print(f'  [WARN] {url} attempt {attempt+1}/{retries}: {e}', file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(RETRY_WAIT)
    return None


def load_skill_dict():
    """
    skill_dict.json を読み込む。EXTRA_SKILLS を補完する。
    Ver 2.1: カテゴリマップも同時に返す (skill_dict, category_map)
    """
    category_map = {}  # {canonical_skill: category_name}
    if not os.path.exists(SKILL_DICT_FILE):
        print(f'[WARN] skill_dict.json not found: {SKILL_DICT_FILE}', file=sys.stderr)
        base = {}
    else:
        with open(SKILL_DICT_FILE, encoding='utf-8') as f:
            raw = json.load(f)
        # カテゴリ辞書形式 {"カテゴリ名": ["スキル1", ...]} か
        # フラット形式 {"pattern": "canonical"} かを自動判別
        base = {}
        if raw:
            first_val = next(iter(raw.values()))
            if isinstance(first_val, list):
                # カテゴリ辞書形式 → フラット化しつつカテゴリ対応を保持
                for cat, skills in raw.items():
                    for sk in skills:
                        base[sk] = sk
                        category_map[sk] = cat
            else:
                base = raw

    for sk in EXTRA_SKILLS:
        if sk not in base:
            base[sk] = sk
        if sk not in category_map:
            # EXTRA_SKILLS のカテゴリ推定 (Ver 2.1)
            if sk in ('vLLM', 'Ray', 'Triton', 'JAX', 'Mojo'):
                category_map[sk] = 'MLエンジニアリング'
            elif sk in ('MCP', 'Agentic RAG'):
                category_map[sk] = 'エージェント・マルチモーダル'
            else:
                category_map[sk] = ''
    return base, category_map


def extract_skills_from_text(text, skill_dict):
    """JDテキストからスキルキーワードを抽出してカウント辞書を返す。"""
    counts = {}
    text_lower = text.lower()
    for pattern, canonical in skill_dict.items():
        regex = r'\b' + re.escape(pattern.lower()) + r'\b'
        hits = len(re.findall(regex, text_lower))
        if hits > 0:
            counts[canonical] = counts.get(canonical, 0) + hits
    return counts


# Ver 2.1: AI関連求人判定用キーワード (厳密マッチ)
AI_JOB_REGEX = re.compile(
    r'\b(ai|ml|llm|llms|genai|gen\s*ai|generative\s*ai|gpt|claude|gemini|'
    r'nlp|machine\s*learning|deep\s*learning|artificial\s*intelligence|'
    r'rag|vector\s*db|transformer|fine[\s\-]?tuning|prompt\s*engineering)\b',
    re.IGNORECASE
)


def _job_text_for_ai(j):
    """Remote OK / WWR 求人の AI判定用テキストを抽出 (タイトル+description+tags)"""
    if not isinstance(j, dict):
        return ''
    parts = [
        str(j.get('position', '') or ''),
        str(j.get('title', '') or ''),
        str(j.get('description', '') or ''),
    ]
    tags = j.get('tags', [])
    if isinstance(tags, list):
        parts.append(' '.join(str(t) for t in tags))
    elif isinstance(tags, str):
        parts.append(tags)
    return ' '.join(parts)


def is_ai_job(j):
    """求人がAI関連か厳密判定する (タイトル/description/tagsへの正規表現マッチ)"""
    text = _job_text_for_ai(j)
    if not text:
        return False
    return bool(AI_JOB_REGEX.search(text))


# ---------------------------------------------------------------------------
# Step 1: Remote OK + WWR 求人取得
# ---------------------------------------------------------------------------
def fetch_remote_ok():
    data = fetch_json(REMOTE_OK_URL)
    if data is None:
        return []
    return [j for j in data if isinstance(j, dict) and 'id' in j]


def fetch_wwr_rss():
    xml_bytes = fetch_xml(WWR_RSS_URL)
    if xml_bytes is None:
        return []
    try:
        root = ET.fromstring(xml_bytes)
        items = root.findall('.//item')
        jobs = []
        for item in items:
            title = (item.findtext('title') or '').strip()
            desc  = (item.findtext('description') or '').strip()
            link  = (item.findtext('link') or '').strip()
            jobs.append({'source': 'wwr', 'title': title, 'description': desc, 'url': link})
        return jobs
    except ET.ParseError as e:
        print(f'[WARN] WWR parse error: {e}', file=sys.stderr)
        return []


# ---------------------------------------------------------------------------
# Step 2: 8社 ATS API 求人取得
# ---------------------------------------------------------------------------
def extract_jd_text_greenhouse(job):
    parts = []
    if job.get('title'):
        parts.append(job['title'])
    content = job.get('content', '')
    if content:
        parts.append(re.sub(r'<[^>]+>', ' ', content))
    return ' '.join(parts)


def extract_jd_text_ashby(job):
    parts = []
    if job.get('title'):
        parts.append(job['title'])
    desc = job.get('descriptionPlain') or job.get('description', '')
    if desc:
        parts.append(re.sub(r'<[^>]+>', ' ', desc))
    return ' '.join(parts)


def extract_jd_text_lever(job):
    parts = []
    if job.get('text'):
        parts.append(job['text'])
    desc = (job.get('descriptionPlain') or
            job.get('description') or
            (job.get('content') or {}).get('body', ''))
    if desc:
        parts.append(re.sub(r'<[^>]+>', ' ', desc))
    return ' '.join(parts)


def aggregate_company_skills(company_cfg, skill_dict):
    slug     = company_cfg['slug']
    ats_type = company_cfg['atsType']
    api_url  = company_cfg['apiUrl']

    print(f'  Fetching {company_cfg["name"]} ({ats_type}) ...')
    data = fetch_json(api_url)
    if data is None:
        print(f'  [ERROR] Failed: {slug}', file=sys.stderr)
        return None

    jobs = []
    if ats_type == 'greenhouse':
        jobs = data.get('jobs', [])
    elif ats_type == 'ashby':
        jobs = data.get('jobs', []) or data.get('jobPostings', [])
    elif ats_type == 'lever':
        jobs = data if isinstance(data, list) else data.get('data', [])

    total_skill_counts = {}
    for job in jobs:
        if ats_type == 'greenhouse':
            text = extract_jd_text_greenhouse(job)
        elif ats_type == 'ashby':
            text = extract_jd_text_ashby(job)
        else:
            text = extract_jd_text_lever(job)

        job_skills = extract_skills_from_text(text, skill_dict)
        # Ver 2.1: 求人数ベース集計 — 同一JD内での重複出現は1件としてカウント
        for sk in job_skills.keys():
            total_skill_counts[sk] = total_skill_counts.get(sk, 0) + 1

    sorted_skills = sorted(total_skill_counts.items(), key=lambda x: x[1], reverse=True)
    top_skills = [{'skill': sk, 'count': cnt} for sk, cnt in sorted_skills[:10]]

    return {
        'slug':       slug,
        'name':       company_cfg['name'],
        'company':    company_cfg['name'],   # fix_ver2.md 互換フィールド (Ver 2.1)
        'atsType':    ats_type,
        'careersUrl': company_cfg['careersUrl'],
        'jobCount':   len(jobs),
        'topSkills':  top_skills,
        'skillCounts': dict(sorted_skills),
    }


# ---------------------------------------------------------------------------
# Step 3: スナップショット差分計算
# ---------------------------------------------------------------------------
def load_prev_snapshot(snap_dir, prefix='', today_str=None):
    """
    Ver 2.1: 前日スナップショットを読み込む。
    - 呼び出し時は「当日スナップショットをまだ保存していない」タイミング想定
      → files[-1] が前日分 = 最も新しい「前日ファイル」
    - 冪等性確保のため当日ファイルは除外 (再実行時の自己参照防止)
    """
    if not os.path.exists(snap_dir):
        return {}
    files = sorted([
        f for f in os.listdir(snap_dir)
        if f.startswith(prefix) and f.endswith('.json')
    ])
    # 当日ファイルを除外 (冪等再実行対策)
    if today_str:
        today_file = f'{today_str}.json'
        files = [f for f in files if f != today_file]
    if not files:
        return {}
    prev_file = files[-1]  # Ver 2.1: 最も新しい残存ファイル = 前日分
    path = os.path.join(snap_dir, prev_file)
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def compute_company_diffs(this_company, prev_snapshot):
    """
    Ver 2.2: bootstrap 検知を追加。
    - prev_snapshot が空 or 当該企業のレコードなし → 全て空配列 (「初日扱い」)
      ※「全スキルが new」と誤表示するのを防ぐ
    """
    slug = this_company['slug']
    prev = prev_snapshot.get(slug, {}).get('skillCounts', {})
    curr = this_company.get('skillCounts', {})

    # Ver 2.2: bootstrap 保護 — prev が空なら diff 計算を skip
    if not prev:
        return [], [], []

    new_skills     = [sk for sk, cnt in curr.items() if cnt > 0 if prev.get(sk, 0) == 0]
    dropped_skills = [sk for sk, cnt in prev.items() if cnt > 0 if curr.get(sk, 0) == 0]

    trending = []
    for sk, cnt in curr.items():
        prev_cnt = prev.get(sk, 0)
        if prev_cnt > 0:
            delta_pct = round((cnt - prev_cnt) / prev_cnt * 100)
            if delta_pct >= 20:
                trending.append({'skill': sk, 'deltaPct': delta_pct})
    trending.sort(key=lambda x: x['deltaPct'], reverse=True)

    return new_skills[:10], dropped_skills[:10], trending[:5]


# ---------------------------------------------------------------------------
# Step 4: カレンダーヒートマップ (直近30日スナップショットから生成)
# ---------------------------------------------------------------------------
def build_calendar_heatmap(today_utc, prev_jobs_snap, ro_jobs, wwr_jobs, skill_dict):
    """
    直近30日分のカレンダーヒートマップを生成する。
    today_utc: datetime.date
    prev_jobs_snap: 前日の jobs snapshot (dict) またはNone
    skill_dict: topSkill計算用のスキル辞書
    """
    today_str = today_utc.isoformat()

    # Ver 2.1: 当日分 AI求人 (厳密判定)
    ai_jobs_today = [j for j in (ro_jobs + wwr_jobs) if is_ai_job(j)]
    todays_total = len(ai_jobs_today)

    # Ver 2.1: 当日AI求人からの最頻出スキルを topSkill に設定
    todays_top_skill = ''
    if ai_jobs_today:
        skill_tally = {}
        for j in ai_jobs_today:
            text = _job_text_for_ai(j)
            for sk in extract_skills_from_text(text, skill_dict).keys():
                skill_tally[sk] = skill_tally.get(sk, 0) + 1
        if skill_tally:
            todays_top_skill = max(skill_tally.items(), key=lambda x: x[1])[0]

    # 既存 summary.json から heatmap を継承し、当日を追加 or 更新
    existing_heatmap = []
    if os.path.exists(SUMMARY_FILE):
        try:
            with open(SUMMARY_FILE, encoding='utf-8') as f:
                old = json.load(f)
            existing_heatmap = old.get('calendarHeatmap', [])
        except (json.JSONDecodeError, OSError):
            pass

    # dict化
    hmap = {e['date']: e for e in existing_heatmap}
    hmap[today_str] = {
        'date': today_str,
        'totalAiJobs': todays_total,
        'topSkill': todays_top_skill,
    }

    # 直近30日だけ残す
    cutoff = (today_utc - datetime.timedelta(days=30)).isoformat()
    result = [v for k, v in sorted(hmap.items()) if k >= cutoff]
    return result


# ---------------------------------------------------------------------------
# Step 5: streakDays 計算
# ---------------------------------------------------------------------------
def compute_streak(today_str):
    """当日含む連続更新日数を計算"""
    if not os.path.exists(SUMMARY_FILE):
        return 1
    try:
        with open(SUMMARY_FILE, encoding='utf-8') as f:
            old = json.load(f)
        prev_streak = old.get('streakDays', 0)
        prev_updated = old.get('lastUpdated', '')[:10]
        today = datetime.date.fromisoformat(today_str)
        if prev_updated:
            prev_date = datetime.date.fromisoformat(prev_updated)
            if (today - prev_date).days <= 1:
                return prev_streak + 1
    except (json.JSONDecodeError, OSError, ValueError):
        pass
    return 1


# ---------------------------------------------------------------------------
# Step 6: monthlyRanking (スナップショットから集計)
# ---------------------------------------------------------------------------
def load_skill_history():
    """
    Ver 2.2: スキル集計履歴を読み込む。
    形式: {"YYYY-MM-DD": {"Python": 11, "Claude": 7, ...}, ...}
    """
    if not os.path.exists(SKILL_HISTORY_FILE):
        return {}
    try:
        with open(SKILL_HISTORY_FILE, encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_skill_history(history, max_days=60):
    """
    Ver 2.2: スキル集計履歴を保存（最新60日保持）。
    """
    sorted_dates = sorted(history.keys(), reverse=True)[:max_days]
    trimmed = {d: history[d] for d in sorted_dates}
    with open(SKILL_HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(trimmed, f, ensure_ascii=False, indent=2)


def build_monthly_ranking(skill_dict, all_jobs, today_str, category_map=None):
    """
    当日取得した AI求人のみ のスキル集計から AI求人スキルランキング（前日比）を生成。

    Ver 2.2 (2026-04-12):
      - prev_counts を日付ベースの skill_counts_history.json から取得
      - 「今日 (today_str) より厳密に前の最新日」を prev として採用
      - 同日再実行は curr_counts で上書き（冪等）
      - summary.json 自己参照を完全解消

    Ver 2.1:
      - is_ai_job() で AI求人に厳密フィルタしてから集計
      - category_map から各スキルのカテゴリを埋める (フィルタボタン動作用)
    """
    if category_map is None:
        category_map = {}

    # Ver 2.1: AI求人のみに絞り込み
    ai_jobs = [j for j in all_jobs if is_ai_job(j)]

    # 当日スキル集計 (求人数ベース: 1求人につき1カウント — presence flag)
    curr_counts = {}
    for j in ai_jobs:
        text = _job_text_for_ai(j)
        job_skills = extract_skills_from_text(text, skill_dict)
        for sk in job_skills.keys():
            curr_counts[sk] = curr_counts.get(sk, 0) + 1

    # Ver 2.2: 履歴ファイルから prev (今日より厳密に前の最新日) を取得
    history = load_skill_history()
    prev_day = None
    for d in sorted(history.keys(), reverse=True):
        if d < today_str:
            prev_day = d
            break
    prev_counts = history.get(prev_day, {}) if prev_day else {}

    # Ver 2.2: 今日のカウントを履歴に書き込み（同日再実行は上書き = 冪等）
    history[today_str] = dict(curr_counts)
    save_skill_history(history)

    sorted_skills = sorted(curr_counts.items(), key=lambda x: x[1], reverse=True)
    ranking = []
    for idx, (skill, curr_cnt) in enumerate(sorted_skills[:20]):
        prev_cnt = prev_counts.get(skill, 0)
        if prev_cnt > 0:
            change_pct = round((curr_cnt - prev_cnt) / prev_cnt * 100, 1)
        else:
            change_pct = 0.0
        change_dir = 'up' if change_pct > 0 else ('down' if change_pct < 0 else 'flat')
        ranking.append({
            'rank':         idx + 1,
            'skill':        skill,
            'category':     category_map.get(skill, ''),   # Ver 2.1
            'currentMonth': curr_cnt,    # legacy key name — 実態は「当日AI求人数」
            'prevMonth':    prev_cnt,    # legacy key name — 実態は「前日AI求人数」
            'changePct':    change_pct,
            'changeDir':    change_dir,
        })
    return ranking


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------
def main():
    utc_now   = datetime.datetime.now(timezone.utc)
    today_str = utc_now.strftime('%Y-%m-%d')
    today_d   = datetime.date.fromisoformat(today_str)

    print(f'[gajt_update] 実行日時 UTC: {utc_now.strftime("%Y-%m-%d %H:%M:%S")}')

    # --- 1. スキル辞書ロード (Ver 2.1: カテゴリマップも取得) ---
    skill_dict, category_map = load_skill_dict()
    print(f'[gajt_update] スキル辞書: {len(skill_dict)} パターン / カテゴリ: {len(set(category_map.values()))}種')

    # --- 2. Remote OK + WWR 取得 ---
    print('[gajt_update] Remote OK 取得中...')
    ro_jobs = fetch_remote_ok()
    print(f'  Remote OK: {len(ro_jobs)} 件')

    print('[gajt_update] WWR RSS 取得中...')
    wwr_jobs = fetch_wwr_rss()
    print(f'  WWR: {len(wwr_jobs)} 件')

    all_jobs = ro_jobs + wwr_jobs
    total_scanned = len(all_jobs)

    # 日次スナップショット保存 (コンパクト版)
    snap_path = os.path.join(SNAPSHOTS_DIR, f'{today_str}.json')
    with open(snap_path, 'w', encoding='utf-8') as f:
        json.dump({
            'date':    today_str,
            'fetched': utc_now.isoformat(),
            'sources': {
                'remote_ok': {'url': REMOTE_OK_URL, 'count': len(ro_jobs)},
                'wwr':       {'url': WWR_RSS_URL,   'count': len(wwr_jobs)},
            },
            'totalJobs': total_scanned,
        }, f, ensure_ascii=False, indent=2)
    print(f'[gajt_update] スナップショット保存: {snap_path}')

    # --- 3. monthlyRanking 生成 (Ver 2.2: today_str で日付履歴から prev 取得) ---
    print('[gajt_update] AI求人スキルランキング集計中...')
    monthly_ranking = build_monthly_ranking(skill_dict, all_jobs, today_str, category_map)
    print(f'  ランキング: {len(monthly_ranking)} スキル')

    # --- 4. カレンダーヒートマップ (Ver 2.1: skill_dict を渡して topSkill 計算) ---
    calendar_heatmap = build_calendar_heatmap(today_d, None, ro_jobs, wwr_jobs, skill_dict)

    # --- 5. streak ---
    streak_days = compute_streak(today_str)

    # --- 6. weeklyTopThree ---
    top3 = monthly_ranking[:3] if len(monthly_ranking) >= 3 else monthly_ranking
    s1 = top3[0]['skill']     if len(top3) > 0 else ''
    p1 = top3[0]['changePct'] if len(top3) > 0 else 0
    s2 = top3[1]['skill']     if len(top3) > 1 else ''
    p2 = top3[1]['changePct'] if len(top3) > 1 else 0
    s3 = top3[2]['skill']     if len(top3) > 2 else ''
    p3 = top3[2]['changePct'] if len(top3) > 2 else 0
    week_of = (today_d - datetime.timedelta(days=today_d.weekday())).isoformat()

    def _fmt_pct(v):
        """Ver 2.1: 正なら '+N.N%'、負なら '-N.N%' (符号は値に任せる)"""
        return ('+' if v >= 0 else '') + ('{0}%'.format(v))

    x_template = (
        '今週の海外AI求人スキルTop3\n'
        '1位: {s1} {f1}\n'
        '2位: {s2} {f2}\n'
        '3位: {s3} {f3}\n'
        '（{w}週 / Remote OK+WWR集計）\n'
        '#AIスキル #生成AI'
    ).format(
        s1=s1, f1=_fmt_pct(p1),
        s2=s2, f2=_fmt_pct(p2),
        s3=s3, f3=_fmt_pct(p3),
        w=week_of,
    )

    weekly_top_three = {
        'weekOf':        week_of,
        'skills':        [r['skill'] for r in top3],
        'percentages':   [r['changePct'] for r in top3],
        'xPostTemplate': x_template,
    }

    # --- 7. KPI (Ver 2.1: 当日単体の比率に統一) ---
    # totalAiJobsThisMonth は月内のAI求人累計 (カレンダー全日合計)
    ai_jobs_total_month = sum(d.get('totalAiJobs', 0) for d in calendar_heatmap)
    # aiJobRatioPct は「当日スキャンした全求人のうちAI求人が占める割合」(当日単体・0-100)
    todays_ai_count = 0
    for _e in calendar_heatmap:
        if _e.get('date') == today_str:
            todays_ai_count = _e.get('totalAiJobs', 0)
            break
    ai_ratio_pct  = round(todays_ai_count / max(total_scanned, 1) * 100, 1)
    up_skills     = [r for r in monthly_ranking if r['changeDir'] == 'up']
    down_skills   = [r for r in monthly_ranking if r['changeDir'] == 'down']
    top_rising    = max(up_skills,   key=lambda x: x['changePct'],  default=None)
    top_falling   = min(down_skills, key=lambda x: x['changePct'],  default=None)
    kpi = {
        'totalAiJobsThisMonth': ai_jobs_total_month,
        'aiJobRatioPct':        ai_ratio_pct,
        'topRisingSkill':  {'name': top_rising['skill'],  'changePct': top_rising['changePct']}  if top_rising  else {'name': '-', 'changePct': 0},
        'topFallingSkill': {'name': top_falling['skill'], 'changePct': top_falling['changePct']} if top_falling else {'name': '-', 'changePct': 0},
    }

    # --- 8. 8社 ATS API 取得 ---
    print('[gajt_update] 8社 ATS API 取得中...')
    prev_comp_snap = load_prev_snapshot(COMP_SNAP_DIR, today_str=today_str)
    companies_result = []
    for cfg in COMPANIES:
        result = aggregate_company_skills(cfg, skill_dict)
        if result is None:
            prev_co = prev_comp_snap.get(cfg['slug'])
            if prev_co:
                companies_result.append(prev_co)
            continue
        new_skills, dropped_skills, trending = compute_company_diffs(result, prev_comp_snap)
        result['newSkills']      = new_skills
        result['droppedSkills']  = dropped_skills
        result['trendingSkills'] = trending
        companies_result.append(result)

    # 企業スナップショット保存
    comp_snap_path = os.path.join(COMP_SNAP_DIR, f'{today_str}.json')
    comp_snap_data = {co['slug']: co for co in companies_result}
    with open(comp_snap_path, 'w', encoding='utf-8') as f:
        json.dump(comp_snap_data, f, ensure_ascii=False, indent=2)
    print(f'[gajt_update] 企業スナップショット保存: {comp_snap_path}')

    # globalNewSkills / globalTopRisers 集計
    all_new = []
    seen_new = set()
    for co in companies_result:
        for sk in co.get('newSkills', []):
            if sk not in seen_new:
                all_new.append(sk)
                seen_new.add(sk)

    risr_map = {}
    for co in companies_result:
        for t in co.get('trendingSkills', []):
            sk = t['skill']
            dp = t['deltaPct']
            risr_map[sk] = max(risr_map.get(sk, 0), dp)
    top_risers = sorted(risr_map.items(), key=lambda x: x[1], reverse=True)[:5]

    company_weekly = {
        'weekOf':          week_of,
        'lastVerified':    today_str,
        'companies':       [
            {k: v for k, v in co.items() if k != 'skillCounts'}
            for co in companies_result
        ],
        'globalNewSkills': all_new[:10],
        'globalTopRisers': [{'skill': sk, 'deltaPct': dp} for sk, dp in top_risers],
    }

    # --- 9. japanLag (静的データ継承 — {entries: [...]} 形式) ---
    japan_lag = None
    if os.path.exists(SUMMARY_FILE):
        try:
            with open(SUMMARY_FILE, encoding='utf-8') as f:
                old = json.load(f)
            jl = old.get('japanLag')
            if jl is not None:
                # フラット配列なら {entries: [...]} にラップ
                if isinstance(jl, list):
                    japan_lag = {'entries': jl}
                else:
                    japan_lag = jl
        except (json.JSONDecodeError, OSError):
            pass

    # --- 10. summary.json 生成 ---
    next_update = (utc_now + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    summary = {
        'lastUpdated': utc_now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'meta': {
            'lastVerified':     today_str,
            'sourceUrls': [
                REMOTE_OK_URL,
                WWR_RSS_URL,
            ],
            'totalJobsScanned': total_scanned,
            'updateCycleHours': 24,
            'nextUpdateISO':    next_update,
        },
        'streakDays':      streak_days,
        'monthlyRanking':  monthly_ranking,
        'calendarHeatmap': calendar_heatmap,
        'weeklyTopThree':  weekly_top_three,
        'kpi':             kpi,
        'companyWeekly':   company_weekly,
    }
    if japan_lag is not None:
        summary['japanLag'] = japan_lag

    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f'[gajt_update] summary.json 保存完了: {SUMMARY_FILE}')
    print(f'  streak={streak_days}, scanned={total_scanned}, ranking={len(monthly_ranking)}件')
    print('[gajt_update] 完了')


if __name__ == '__main__':
    main()
