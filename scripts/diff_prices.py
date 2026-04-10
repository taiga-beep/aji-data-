#!/usr/bin/env python3
"""
diff_prices.py — OpenRouter API 価格差分計算スクリプト
毎日GitHub Actionsから実行される。

処理フロー:
1. OpenRouter API /api/v1/models からJSON取得
2. data/snapshots/YYYY-MM-DD.json に保存
3. 前日のスナップショットと比較して差分を計算
4. data/latest.json, data/diff/YYYY-MM-DD.json, data/summary.json を生成

価格単位: USD / 100万トークン ($/Mトークン)
出典: https://openrouter.ai/api/v1/models
lastVerified: 2026-04-10
"""

import json
import os
import sys
import math
import requests
from datetime import datetime, timezone, timedelta

# --- 定数 ---
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/models"
DATA_DIR = "data"
SNAPSHOTS_DIR = os.path.join(DATA_DIR, "snapshots")
DIFF_DIR = os.path.join(DATA_DIR, "diff")
SUMMARY_FILE = os.path.join(DATA_DIR, "summary.json")
LATEST_FILE = os.path.join(DATA_DIR, "latest.json")

# 価格推移を追跡するデフォルト主要モデル
TRACKED_MODELS = [
    "openai/gpt-4o",
    "openai/gpt-4o-mini",
    "anthropic/claude-3.5-sonnet",
    "anthropic/claude-3-haiku",
    "google/gemini-1.5-pro",
    "google/gemini-1.5-flash",
    "meta-llama/llama-3.1-70b-instruct",
    "meta-llama/llama-3.1-405b-instruct",
    "mistralai/mistral-large",
    "mistralai/mixtral-8x7b-instruct",
]

# 価格帯カテゴリ閾値 ($/Mトークン, inputPrice基準)
PRICE_FLAGSHIP = 10.0   # $10以上
PRICE_MID = 1.0         # $1〜$10
PRICE_BUDGET = 0.0001   # $0超〜$1未満
# $0以下 = free


def get_jst_now():
    """現在時刻をJSTで返す"""
    return datetime.now(timezone(timedelta(hours=9)))


def fetch_openrouter_models():
    """OpenRouter APIからモデル一覧を取得"""
    headers = {"HTTP-Referer": "https://ai-japan-index.com/openrouter-price-tracker/"}
    response = requests.get(OPENROUTER_API_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_model(raw):
    """
    OpenRouter APIのモデルデータをsummary.json形式に変換

    OpenRouter APIフィールドマッピング:
    - pricing.prompt ($/token) -> inputPrice ($/Mトークン = x1,000,000)
    - pricing.completion ($/token) -> outputPrice ($/Mトークン)
    - context_length -> contextLength
    - id (e.g. "openai/gpt-4o") -> id, provider(スラッシュ前部分)
    """
    model_id = raw.get("id", "")
    provider = model_id.split("/")[0] if "/" in model_id else "unknown"
    name = raw.get("name", model_id)

    pricing = raw.get("pricing", {})
    try:
        prompt_per_token = float(pricing.get("prompt", "0") or "0")
        completion_per_token = float(pricing.get("completion", "0") or "0")
    except (ValueError, TypeError):
        prompt_per_token = 0.0
        completion_per_token = 0.0

    # $/token -> $/Mトークン (x1,000,000)
    input_price = round(prompt_per_token * 1_000_000, 6)
    output_price = round(completion_per_token * 1_000_000, 6)

    context_length = int(raw.get("context_length", 0) or 0)

    # 価格帯カテゴリ
    if input_price <= 0:
        price_category = "free"
    elif input_price >= PRICE_FLAGSHIP:
        price_category = "flagship"
    elif input_price >= PRICE_MID:
        price_category = "mid"
    else:
        price_category = "budget"

    return {
        "id": model_id,
        "name": name,
        "provider": provider,
        "inputPrice": input_price,
        "outputPrice": output_price,
        "contextLength": context_length,
        "priceCategory": price_category,
        "prevInputPrice": None,
        "prevOutputPrice": None,
        "priceChangePercent": None,
        "isNew": False,
        "isRemoved": False,
        "created": raw.get("created", None),
    }


def load_snapshot(date_str):
    """指定日のスナップショットを読み込む。なければNoneを返す"""
    path = os.path.join(SNAPSHOTS_DIR, f"{date_str}.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_snapshot(date_str, models):
    """スナップショットを保存"""
    os.makedirs(SNAPSHOTS_DIR, exist_ok=True)
    path = os.path.join(SNAPSHOTS_DIR, f"{date_str}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(models, f, ensure_ascii=False, indent=2)


def compute_diff(current_models, prev_snapshot):
    """
    前日スナップショットと比較して差分を計算。
    戻り値: 差分が付与されたモデルリスト
    """
    if prev_snapshot is None:
        return current_models

    prev_map = {m["id"]: m for m in prev_snapshot}

    result = []
    for m in current_models:
        prev = prev_map.get(m["id"])
        m_copy = dict(m)

        if prev is None:
            # 新規モデル
            m_copy["isNew"] = True
        else:
            prev_in = prev.get("inputPrice", 0)
            curr_in = m_copy["inputPrice"]
            if prev_in > 0 and curr_in != prev_in:
                m_copy["prevInputPrice"] = prev_in
                m_copy["prevOutputPrice"] = prev.get("outputPrice")
                m_copy["priceChangePercent"] = round((curr_in - prev_in) / prev_in * 100, 2)
        result.append(m_copy)

    # 削除モデル検出
    current_ids = {m["id"] for m in current_models}
    for prev_m in prev_snapshot:
        if prev_m["id"] not in current_ids:
            removed = dict(prev_m)
            removed["isRemoved"] = True
            result.append(removed)

    return result


def build_history(current_date_str, models):
    """
    既存のsummary.jsonからhistoryを読み込み、当日分を追加して返す。
    直近30日分のみ保持。
    """
    tracked_ids = TRACKED_MODELS
    current_map = {m["id"]: m for m in models}

    # 既存summary.jsonから読み込み
    history = {"dates": [], "trackedModels": {}}
    if os.path.exists(SUMMARY_FILE):
        try:
            with open(SUMMARY_FILE, "r", encoding="utf-8") as f:
                old = json.load(f)
            history = old.get("history", history)
        except Exception:
            pass

    # 当日日付を追加
    if current_date_str not in history["dates"]:
        history["dates"].append(current_date_str)

    # 30日を超えたら古い日付を削除
    if len(history["dates"]) > 30:
        removed_dates = history["dates"][:-30]
        history["dates"] = history["dates"][-30:]
        for rm_date in removed_dates:
            for tm in history["trackedModels"].values():
                # 削除された日付のインデックスを特定するのは複雑なため
                # 単純にhistory再構築は行わず日付リストのみ更新
                pass

    # 当日の価格を追加
    date_index = history["dates"].index(current_date_str)
    for model_id in tracked_ids:
        if model_id not in history["trackedModels"]:
            history["trackedModels"][model_id] = {
                "inputPrices": [None] * len(history["dates"]),
                "outputPrices": [None] * len(history["dates"]),
            }

        tm = history["trackedModels"][model_id]
        # 配列長を日付リストに合わせる
        while len(tm["inputPrices"]) < len(history["dates"]):
            tm["inputPrices"].append(None)
            tm["outputPrices"].append(None)

        # 当日の価格を設定
        current_model = current_map.get(model_id)
        if current_model:
            tm["inputPrices"][date_index] = current_model["inputPrice"]
            tm["outputPrices"][date_index] = current_model["outputPrice"]

    return history


def count_changes(models):
    """変動サマリを集計"""
    price_down = sum(1 for m in models if m.get("priceChangePercent") is not None and m["priceChangePercent"] < 0)
    price_up   = sum(1 for m in models if m.get("priceChangePercent") is not None and m["priceChangePercent"] > 0)
    new_models = sum(1 for m in models if m.get("isNew", False))
    removed    = sum(1 for m in models if m.get("isRemoved", False))
    return {
        "priceDown": price_down,
        "priceUp": price_up,
        "newModels": new_models,
        "removedModels": removed,
    }


def main():
    jst_now = get_jst_now()
    today_str = jst_now.strftime("%Y-%m-%d")
    yesterday_str = (jst_now - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"[diff_prices.py] 実行日時 (JST): {jst_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[diff_prices.py] OpenRouter API からデータ取得中...")

    # APIからデータ取得
    try:
        raw_data = fetch_openrouter_models()
    except Exception as e:
        print(f"[ERROR] APIフェッチ失敗: {e}")
        sys.exit(1)

    raw_models = raw_data.get("data", [])
    print(f"[diff_prices.py] 取得モデル数: {len(raw_models)}")

    # パース
    current_models = [parse_model(m) for m in raw_models]

    # 前日スナップショット読み込み
    prev_snapshot = load_snapshot(yesterday_str)
    if prev_snapshot is None:
        print(f"[diff_prices.py] 前日スナップショットなし ({yesterday_str})")
    else:
        print(f"[diff_prices.py] 前日スナップショット読み込み: {len(prev_snapshot)} モデル")

    # 差分計算
    models_with_diff = compute_diff(current_models, prev_snapshot)

    # 変動サマリ
    changes = count_changes(models_with_diff)
    print(f"[diff_prices.py] 値下げ: {changes['priceDown']}件, 値上げ: {changes['priceUp']}件, "
          f"新規: {changes['newModels']}件, 削除: {changes['removedModels']}件")

    # 当日スナップショット保存（差分なしの純粋なモデルリスト）
    save_snapshot(today_str, current_models)
    print(f"[diff_prices.py] スナップショット保存: {SNAPSHOTS_DIR}/{today_str}.json")

    # 差分ファイル保存
    os.makedirs(DIFF_DIR, exist_ok=True)
    diff_path = os.path.join(DIFF_DIR, f"{today_str}.json")
    changed_only = [m for m in models_with_diff if m.get("priceChangePercent") is not None or m.get("isNew") or m.get("isRemoved")]
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump(changed_only, f, ensure_ascii=False, indent=2)
    print(f"[diff_prices.py] 差分ファイル保存: {diff_path}")

    # latest.json 保存
    with open(LATEST_FILE, "w", encoding="utf-8") as f:
        json.dump(models_with_diff, f, ensure_ascii=False, indent=2)
    print(f"[diff_prices.py] latest.json 保存完了")

    # history構築
    history = build_history(today_str, current_models)

    # summary.json 生成
    summary = {
        "lastUpdated": jst_now.strftime("%Y-%m-%dT%H:%M:%SZ").replace(jst_now.strftime("%H:%M:%S"), "07:00:00"),
        "totalModels": len([m for m in models_with_diff if not m.get("isRemoved", False)]),
        "todayChanges": changes,
        "models": models_with_diff,
        "history": history,
    }

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"[diff_prices.py] summary.json 保存完了 ({SUMMARY_FILE})")
    print("[diff_prices.py] 完了")


if __name__ == "__main__":
    main()
