#!/usr/bin/env python3
import json
import math
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MASTER = ROOT / "data/tesla-master-db.json"
TIMELINE = ROOT / "data/tesla-timeline.json"
SEED = ROOT / "data/tesla-comments-seed.json"
MAX_COMMENTS = 20


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def dump_json(path: Path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def extract_status_id(url: str):
    if not url:
        return None
    m = re.search(r"/status/(\d+)", url)
    return m.group(1) if m else None


def info_density_score(text: str):
    text = (text or "").strip()
    if not text:
        return 0.0
    tokens = re.findall(r"[A-Za-z0-9\-\.]+", text)
    uniq = len(set(t.lower() for t in tokens))
    length = len(text)
    numbers = len(re.findall(r"\d", text))
    return min(10.0, uniq / 12.0 + min(4.0, length / 80.0) + numbers * 0.08)


def rank_comments(comments):
    scored = []
    for c in comments:
        likes = int(c.get("likes", 0) or 0)
        density = info_density_score(c.get("text_en", ""))
        score = likes * 0.75 + density * 30
        cc = {
            "author": c.get("author", ""),
            "handle": c.get("handle", ""),
            "time": c.get("time", ""),
            "text_en": c.get("text_en", ""),
            "text_zh": c.get("text_zh", ""),
            "likes": likes,
        }
        scored.append((score, cc))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [x[1] for x in scored[:MAX_COMMENTS]]


def attach_comments(events, by_status):
    for e in events:
        sid = extract_status_id(e.get("source", ""))
        if sid and sid in by_status:
            e["comments"] = rank_comments(by_status[sid])
        else:
            e.pop("comments", None)


if __name__ == "__main__":
    master = load_json(MASTER)
    timeline = load_json(TIMELINE)
    seed = load_json(SEED)
    by_status = seed.get("by_status_id", {})

    attach_comments(master.get("events", []), by_status)
    attach_comments(timeline.get("events", []), by_status)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    master.setdefault("meta", {})["last_updated"] = now
    timeline.setdefault("meta", {})["last_updated"] = now
    timeline.setdefault("meta", {})["note"] = "由 tesla-master-db.json 自动镜像（含推文评论）"

    dump_json(MASTER, master)
    dump_json(TIMELINE, timeline)

    print(f"Updated Tesla DB + timeline with comments at {now}")
