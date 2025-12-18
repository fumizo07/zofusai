# preview_api.py
from __future__ import annotations

import os
import sqlite3
from flask import Blueprint, jsonify, request

preview_api = Blueprint("preview_api", __name__)

def _get_db_path() -> str:
    # あなたのプロジェクトに合わせて調整してください
    # 例: DB_PATH 環境変数 or data/app.db
    return os.environ.get("DB_PATH", "data/app.db")

@preview_api.get("/api/post_preview")
def api_post_preview():
    thread_url = (request.args.get("thread_url") or "").strip()
    post_no_raw = (request.args.get("post_no") or "").strip()

    if not thread_url or not post_no_raw.isdigit():
        return jsonify({"error": "bad_request"}), 400

    post_no = int(post_no_raw)

    db_path = _get_db_path()
    if not os.path.exists(db_path):
        return jsonify({"error": "db_not_found"}), 500

    # ★前提：posts テーブルに thread_url / post_no / body / posted_at がある
    # 違うならここだけ合わせればOK
    sql = """
    SELECT body, posted_at
    FROM posts
    WHERE thread_url = ? AND post_no = ?
    LIMIT 1
    """

    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        row = con.execute(sql, (thread_url, post_no)).fetchone()
    finally:
        try:
            con.close()
        except Exception:
            pass

    if row is None:
        return jsonify({"error": "not_found"}), 404

    body = row["body"] if row["body"] is not None else ""
    posted_at = row["posted_at"] if row["posted_at"] is not None else ""

    # ツールチップ用なので長すぎたら軽く切る（必要なら調整）
    if len(body) > 4000:
        body = body[:4000] + "\n…（省略）"

    return jsonify({
        "ok": True,
        "thread_url": thread_url,
        "post_no": post_no,
        "posted_at": posted_at,
        "body": body,
    })
