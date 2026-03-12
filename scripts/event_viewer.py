#!/usr/bin/env python3
"""
HyperQueue Event Viewer
Generates a static HTML page from a newline-delimited JSON event export.

Usage: python scripts/event_viewer.py <events.json> [output.html]
"""

import json
import sys
import html
from pathlib import Path
from collections import Counter

EVENT_COLORS = {
    "server-start": "#818cf8",
    "server-stop": "#a78bfa",
    "worker-connected": "#34d399",
    "worker-lost": "#fbbf24",
    "job-created": "#60a5fa",
    "job-completed": "#4ade80",
    "task-started": "#22d3ee",
    "task-finished": "#2dd4bf",
    "task-failed": "#f87171",
    "task-canceled": "#fb923c",
    "autoalloc-queue-created": "#c084fc",
    "autoalloc-queue-removed": "#f472b6",
    "autoalloc-allocation-queued": "#a3e635",
    "autoalloc-allocation-started": "#facc15",
    "autoalloc-allocation-finished": "#94a3b8",
}
DEFAULT_COLOR = "#94a3b8"


def get_event_type(event: dict) -> str:
    return event.get("event", {}).get("type", "unknown")


def get_event_summary(event: dict) -> str:
    e = event.get("event", {})
    etype = e.get("type", "unknown")

    if etype == "server-start":
        return f"Server started (uid: {e.get('server_uid', '?')})"
    if etype == "server-stop":
        return "Server stopped"
    if etype == "worker-connected":
        cfg = e.get("configuration", {})
        host = cfg.get("hostname", "?")
        return f"Worker {e.get('id', '?')} connected ({host})"
    if etype == "worker-lost":
        return f"Worker {e.get('id', '?')} lost (reason: {e.get('reason', '?')})"
    if etype == "job-created":
        name = e.get("job_desc", {}).get("name") or ""
        label = f" ({name!r})" if name else ""
        return f"Job {e.get('job', '?')} created{label}"
    if etype == "job-completed":
        return f"Job {e.get('job', '?')} completed"
    if etype == "task-started":
        return f"Task {e.get('job', '?')}@{e.get('task', '?')} started on worker {e.get('worker', '?')}"
    if etype == "task-finished":
        return f"Task {e.get('job', '?')}@{e.get('task', '?')} finished"
    if etype == "task-failed":
        return f"Task {e.get('job', '?')}@{e.get('task', '?')} FAILED: {e.get('error', '')[:60]}"
    if etype == "task-canceled":
        tasks = e.get("tasks", [])
        return f"{len(tasks)} task(s) canceled"
    if etype == "autoalloc-queue-created":
        p = e.get("params", {})
        return f"Autoalloc queue {e.get('queue-id', '?')} created ({p.get('manager', '?')})"
    if etype == "autoalloc-queue-removed":
        return f"Autoalloc queue {e.get('queue-id', '?')} removed"
    if etype == "autoalloc-allocation-queued":
        return f"Allocation {e.get('allocation-id', '?')} queued (queue {e.get('queue-id', '?')}, {e.get('worker-count', '?')} workers)"
    if etype == "autoalloc-allocation-started":
        return f"Allocation {e.get('allocation-id', '?')} started (queue {e.get('queue-id', '?')})"
    if etype == "autoalloc-allocation-finished":
        return f"Allocation {e.get('allocation-id', '?')} finished (queue {e.get('queue-id', '?')})"

    # Fallback: show type + first few keys
    extra = {k: v for k, v in e.items() if k != "type"}
    if extra:
        snippet = ", ".join(f"{k}={v}" for k, v in list(extra.items())[:3])
        return f"{etype}: {snippet}"
    return etype


def load_events(path: Path) -> list[dict]:
    events = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Warning: skipping line {lineno}: {e}", file=sys.stderr)
    return events


CATEGORY_COLORS = [
    "#f97316", "#a855f7", "#14b8a6", "#eab308",
    "#ec4899", "#6366f1", "#10b981", "#ef4444",
]


def _fmt_res_val(v: int) -> str:
    n = v / 10_000
    return str(int(n)) if n == int(n) else f"{n:.4g}"


def _variants_key(variants: list) -> tuple:
    """Canonical hashable key from resource variants (ignores min_time/n_nodes)."""
    result = []
    for v in variants:
        res = tuple(sorted(
            (r["resource"], next(iter(r.get("policy", {}).values()), 0))
            for r in v.get("resources", [])
        ))
        result.append(res)
    return tuple(result)


def _describe_variants(variants: list) -> str:
    parts = []
    for i, v in enumerate(variants):
        res_strs = [
            f"{r['resource']}: {_fmt_res_val(next(iter(r.get('policy', {}).values()), 0))}"
            for r in sorted(v.get("resources", []), key=lambda r: r["resource"])
        ]
        prefix = f"v{i}: " if len(variants) > 1 else ""
        parts.append(prefix + ", ".join(res_strs))
    return " | ".join(parts)


def build_categories(events: list[dict]) -> tuple[dict, dict]:
    """
    Returns (job_to_cat, cat_info) where:
      job_to_cat : {job_id -> "T1"}
      cat_info   : {"T1" -> {name, color, label}}
    """
    key_to_cat: dict = {}
    job_to_cat: dict = {}
    for event in events:
        e = event.get("event", {})
        if e.get("type") != "job-created":
            continue
        variants = (
            e.get("submit_desc", {})
            .get("task_desc", {})
            .get("resources", {})
            .get("variants", [])
        )
        key = _variants_key(variants)
        if key not in key_to_cat:
            n = len(key_to_cat) + 1
            key_to_cat[key] = {
                "name": f"T{n}",
                "color": CATEGORY_COLORS[(n - 1) % len(CATEGORY_COLORS)],
                "label": _describe_variants(variants),
            }
        job_to_cat[e["job"]] = key_to_cat[key]["name"]
    cat_info = {v["name"]: v for v in key_to_cat.values()}
    return job_to_cat, cat_info


def compute_chart_data(events: list[dict], job_to_cat: dict) -> tuple[list, dict, dict, dict, list]:
    """
    Returns (timeline, worker_lifetimes, running_timelines, pending_timelines,
             alloc_queued_timeline):
      timeline              : [[ms, count], ...] step-function of active worker count
      worker_lifetimes      : {worker_id: [connect_ms, disconnect_ms_or_null]}
      running_timelines     : {cat: [[ms, count], ...]} tasks currently executing
      pending_timelines     : {cat: [[ms, count], ...]} tasks submitted but not yet started
      alloc_queued_timeline : [[ms, count], ...] allocations waiting in queue
    """
    from datetime import datetime

    def parse_ms(t: str) -> int:
        return int(datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp() * 1000)

    timeline: list = []
    worker_lifetimes: dict = {}
    worker_count = 0

    running_counts: dict = {}  # cat -> current running count
    pending_counts: dict = {}  # cat -> current pending count
    running_timelines: dict = {}
    pending_timelines: dict = {}
    running_tasks: dict = {}  # (job_id, task_id) -> cat

    alloc_queued_count = 0
    alloc_queued_tl: list = []

    first_ms = parse_ms(events[0].get("time", "")) if events else 0
    last_ms = parse_ms(events[-1].get("time", "")) if events else 0

    def push(tl_dict: dict, counts: dict, cat: str, ms: int, old: int, new: int) -> None:
        tl = tl_dict.setdefault(cat, [])
        tl.append([ms, old])
        tl.append([ms, new])
        counts[cat] = new

    for event in events:
        e = event.get("event", {})
        t = event.get("time", "")
        if not t:
            continue
        ms = parse_ms(t)
        etype = e.get("type", "")

        if etype == "worker-connected":
            timeline.append([ms, worker_count])
            worker_count += 1
            timeline.append([ms, worker_count])
            worker_lifetimes[e["id"]] = [ms, None]
        elif etype == "worker-lost":
            timeline.append([ms, worker_count])
            worker_count -= 1
            timeline.append([ms, worker_count])
            wid = e.get("id")
            if wid in worker_lifetimes:
                worker_lifetimes[wid][1] = ms
        elif etype == "job-created":
            job_id = e.get("job")
            cat = job_to_cat.get(job_id)
            if cat:
                ranges = (e.get("submit_desc", {}).get("task_desc", {})
                          .get("ids", {}).get("ranges", []))
                n_tasks = sum(r.get("count", 0) for r in ranges)
                old = pending_counts.get(cat, 0)
                push(pending_timelines, pending_counts, cat, ms, old, old + n_tasks)
        elif etype == "task-started":
            job_id, task_id = e.get("job"), e.get("task")
            cat = job_to_cat.get(job_id)
            if cat:
                running_tasks[(job_id, task_id)] = cat
                # pending -1
                old_p = pending_counts.get(cat, 0)
                push(pending_timelines, pending_counts, cat, ms, old_p, max(0, old_p - 1))
                # running +1
                old_r = running_counts.get(cat, 0)
                push(running_timelines, running_counts, cat, ms, old_r, old_r + 1)
        elif etype in ("task-finished", "task-failed"):
            key = (e.get("job"), e.get("task"))
            cat = running_tasks.pop(key, None)
            if cat:
                old_r = running_counts.get(cat, 0)
                push(running_timelines, running_counts, cat, ms, old_r, max(0, old_r - 1))
        elif etype == "task-canceled":
            for task in e.get("tasks", []):
                key = (task.get("job_id"), task.get("job_task_id"))
                cat = running_tasks.pop(key, None)
                if cat:
                    # was running
                    old_r = running_counts.get(cat, 0)
                    push(running_timelines, running_counts, cat, ms, old_r, max(0, old_r - 1))
                else:
                    # was pending
                    cat = job_to_cat.get(task.get("job_id"))
                    if cat:
                        old_p = pending_counts.get(cat, 0)
                        push(pending_timelines, pending_counts, cat, ms, old_p, max(0, old_p - 1))
        elif etype == "autoalloc-allocation-queued":
            alloc_queued_tl.append([ms, alloc_queued_count])
            alloc_queued_count += 1
            alloc_queued_tl.append([ms, alloc_queued_count])
        elif etype == "autoalloc-allocation-started":
            alloc_queued_tl.append([ms, alloc_queued_count])
            alloc_queued_count = max(0, alloc_queued_count - 1)
            alloc_queued_tl.append([ms, alloc_queued_count])

    def extend(tl_dict, counts):
        for cat, tl in tl_dict.items():
            if tl[0][0] > first_ms:
                tl.insert(0, [first_ms, 0])
            if tl[-1][0] < last_ms:
                tl.append([last_ms, counts[cat]])

    if not timeline:
        timeline = [[first_ms, 0], [last_ms, 0]]
    else:
        if timeline[0][0] > first_ms:
            timeline.insert(0, [first_ms, 0])
        if timeline[-1][0] < last_ms:
            timeline.append([last_ms, worker_count])

    extend(running_timelines, running_counts)
    extend(pending_timelines, pending_counts)

    for tl, count in [(alloc_queued_tl, alloc_queued_count)]:
        if tl:
            if tl[0][0] > first_ms:
                tl.insert(0, [first_ms, 0])
            if tl[-1][0] < last_ms:
                tl.append([last_ms, count])
        else:
            tl += [[first_ms, 0], [last_ms, 0]]

    return timeline, worker_lifetimes, running_timelines, pending_timelines, alloc_queued_tl


def _format_makespan(events: list[dict]) -> str:
    if len(events) < 2:
        return ""
    from datetime import datetime
    def parse_ms(t: str) -> int:
        return int(datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp() * 1000)

    first_ms = parse_ms(events[0].get("time", ""))
    last_ms = parse_ms(events[-1].get("time", ""))
    ms = last_ms - first_ms
    seconds = ms / 1000
    if seconds < 120:
        return f"{seconds:.1f} s" if seconds != int(seconds) else f"{int(seconds)} s"
    minutes = seconds / 60
    hours = minutes / 60
    if hours < 36:
        h = int(hours)
        m = int(minutes) % 60
        return f"{h}h:{m:02d}m"
    days = int(hours // 24)
    h_rem = int(hours % 24)
    return f"{days} days {h_rem} hours"


def render_html(events: list[dict], source_file: str) -> str:
    job_to_cat, cat_info = build_categories(events)
    job_to_cat_js = json.dumps(job_to_cat)
    cat_info_js = json.dumps(cat_info)

    timeline, worker_lifetimes, running_timelines, pending_timelines, alloc_queued_tl = compute_chart_data(events, job_to_cat)
    timeline_js = json.dumps(timeline)
    worker_lifetimes_js = json.dumps(worker_lifetimes)
    task_timelines_js = json.dumps(running_timelines)
    pending_timelines_js = json.dumps(pending_timelines)
    alloc_queued_js = json.dumps(alloc_queued_tl)

    counts = Counter(get_event_type(e) for e in events)
    all_types = sorted(counts.keys())

    legend_items = []
    for etype in all_types:
        color = EVENT_COLORS.get(etype, DEFAULT_COLOR)
        legend_items.append(
            f'<span class="legend-item" data-type="{html.escape(etype)}" '
            f'style="--color:{color}">'
            f'<span class="legend-dot"></span>'
            f'{html.escape(etype.replace("autoalloc-", "aa-"))} <span class="legend-count">({counts[etype]})</span>'
            f"</span>"
        )

    rows = []
    for i, event in enumerate(events):
        etype = get_event_type(event)
        color = EVENT_COLORS.get(etype, DEFAULT_COLOR)
        time_str = event.get("time", "")
        summary = get_event_summary(event)
        json_str = html.escape(json.dumps(event, indent=2))
        rows.append(
            f'<tr class="event-row" data-type="{html.escape(etype)}" data-index="{i}" '
            f'style="--color:{color}">'
            f'<td class="idx">{i + 1}</td>'
            f'<td class="time"><span class="abs-time">{html.escape(time_str)}</span><span class="rel-time"></span></td>'
            f'<td class="type-badge"><span class="badge" style="color:{color}">{html.escape(etype.replace("autoalloc-", "aa-"))}</span></td>'
            f'<td class="summary">{html.escape(summary)}</td>'
            f'<td class="json-data" hidden>{json_str}</td>'
            f"</tr>"
        )

    rows_html = "\n".join(rows)
    legend_html = "\n".join(legend_items)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HyperQueue Events — {html.escape(source_file)}</title>
<style>
  :root {{
    --bg: #0f172a;
    --surface: #1e293b;
    --surface2: #273549;
    --border: #334155;
    --text: #e2e8f0;
    --text-muted: #94a3b8;
    --accent: #38bdf8;
    --radius: 6px;
    --font-mono: "Cascadia Code", "Fira Code", "JetBrains Mono", ui-monospace, monospace;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{ height: 100%; }}
  body {{ background: var(--bg); color: var(--text); font-family: system-ui, sans-serif; font-size: 14px; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }}

  .chart-container {{
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    border-bottom: 1px solid var(--border);
    background: var(--bg);
  }}
  .chart-canvas-wrap {{
    height: 90px;
    position: relative;
    flex-shrink: 0;
  }}
  #chart, #worker-chart, #zoom-chart {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; }}
  #chart {{ cursor: crosshair; }}
  #zoom-chart, #worker-chart {{ cursor: pointer; }}
  .zoom-chart-header {{
    position: absolute; top: 0; left: 0; right: 0; height: 20px; z-index: 2;
    display: flex; align-items: center; padding: 0 8px; gap: 8px;
    background: rgba(15,23,42,0.75); border-bottom: 1px solid var(--border);
    font-size: 10px; font-family: var(--font-mono); color: var(--text-muted);
  }}
  .zoom-range-btn {{
    background: none; border: 1px solid var(--border); border-radius: 3px;
    color: var(--accent); font: inherit; font-size: 10px; cursor: pointer;
    padding: 0 5px; line-height: 17px; white-space: nowrap;
  }}
  .zoom-range-btn:hover {{ border-color: var(--accent); background: color-mix(in srgb, var(--accent) 10%, transparent); }}
  .zoom-range-wrap {{ position: relative; }}
  .zoom-range-popup {{
    display: none; position: absolute; top: calc(100% + 3px); left: 0; z-index: 200;
    background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
    box-shadow: 0 6px 20px rgba(0,0,0,0.5); min-width: 100px; padding: 3px 0;
  }}
  .zoom-range-popup.open {{ display: block; }}
  .zoom-range-option {{
    padding: 5px 12px; font-size: 11px; font-family: var(--font-mono);
    color: var(--text); cursor: pointer; white-space: nowrap;
  }}
  .zoom-range-option:hover {{ background: color-mix(in srgb, var(--accent) 12%, transparent); }}
  .zoom-range-option.selected {{ color: var(--accent); }}
  .worker-chart-header {{
    position: absolute; top: 0; left: 0; right: 0; height: 22px; z-index: 2;
    display: flex; align-items: center; justify-content: space-between;
    padding: 0 8px; gap: 8px;
    background: rgba(15,23,42,0.88); border-bottom: 1px solid var(--border);
    font-size: 11px; font-family: var(--font-mono); color: var(--text-muted);
  }}
  .worker-chart-close {{
    background: none; border: none; color: var(--text-muted); cursor: pointer;
    font-size: 14px; line-height: 1; padding: 0 2px; flex-shrink: 0;
  }}
  .worker-chart-close:hover {{ color: var(--text); }}
  .tree-tag.tag-worker {{ cursor: pointer; }}
  .tree-tag.tag-worker:hover {{ filter: brightness(1.25); }}
  .tree-tag.tag-worker.worker-selected {{ outline: 2px solid #fff; outline-offset: 1px; }}
  .chart-legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    padding: 3px 10px;
    border-top: 1px solid var(--border);
    min-height: 24px;
    align-items: center;
  }}
  .chart-legend-item {{
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 10px;
    font-family: var(--font-mono);
    color: var(--text-muted);
    background: rgba(15,23,42,0.75);
    padding: 1px 5px;
    border-radius: 3px;
    cursor: pointer;
    user-select: none;
    transition: opacity 0.15s;
  }}
  .chart-legend-item:hover {{ color: var(--text); }}
  .chart-legend-item.hidden-line {{ opacity: 0.35; text-decoration: line-through; }}
  .chart-legend-line {{
    width: 14px;
    height: 2px;
    flex-shrink: 0;
    border-radius: 1px;
  }}

  header {{
    padding: 16px 24px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
    background: var(--surface);
  }}
  header h1 {{ font-size: 1.1rem; color: var(--accent); white-space: nowrap; }}
  header .meta {{ color: var(--text-muted); font-size: 12px; }}

  .toolbar {{
    padding: 12px 24px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    display: flex;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
  }}
  .search {{
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    color: var(--text);
    padding: 6px 10px;
    font-size: 13px;
    width: 260px;
  }}
  .search:focus {{ outline: none; border-color: var(--accent); }}
  .legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 10px 24px;
    border-bottom: 1px solid var(--border);
    background: var(--surface2);
  }}
  .legend.collapsed {{ display: none; }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 5px;
    cursor: pointer;
    padding: 3px 8px;
    border-radius: 999px;
    border: 1px solid transparent;
    font-size: 12px;
    user-select: none;
    transition: border-color 0.15s;
  }}
  .legend-item:hover {{ border-color: var(--color); }}
  .legend-item.active {{ border-color: var(--color); background: color-mix(in srgb, var(--color) 15%, transparent); }}
  .legend-dot {{ width: 8px; height: 8px; border-radius: 50%; background: var(--color); flex-shrink: 0; }}
  .legend-count {{ color: var(--text-muted); }}

  .main {{ display: flex; flex: 1; min-height: 0; overflow: hidden; }}
  .table-pane {{ flex: 1; min-width: 200px; overflow: auto; }}

  .resizer {{
    width: 5px;
    flex-shrink: 0;
    background: var(--border);
    cursor: col-resize;
    transition: background 0.15s;
    position: relative;
    z-index: 10;
  }}
  .resizer:hover, .resizer.dragging {{ background: var(--accent); }}

  table {{ width: 100%; border-collapse: collapse; }}
  thead th {{
    position: sticky; top: 0;
    background: var(--surface);
    padding: 8px 12px;
    text-align: left;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }}
  .event-row {{ cursor: pointer; border-bottom: 1px solid color-mix(in srgb, var(--border) 40%, transparent); transition: background 0.1s; }}
  .event-row:hover {{ background: color-mix(in srgb, var(--color) 8%, transparent); }}
  .event-row.selected {{ background: color-mix(in srgb, var(--color) 18%, transparent); }}
  .event-row.hidden {{ display: none; }}
  td {{ padding: 7px 12px; vertical-align: middle; }}
  td.idx {{ color: var(--text-muted); font-size: 11px; text-align: right; width: 48px; }}
  td.time {{ font-family: var(--font-mono); font-size: 11px; color: var(--text-muted); white-space: nowrap; }}
  .rel-time {{ font-size: 10px; color: var(--text-muted); opacity: 0.75; }}
  td.summary {{ color: var(--text); }}
  .badge {{
    display: inline-block;
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 600;
    white-space: nowrap;
  }}

  .detail-pane {{
    width: 50%;
    min-width: 150px;
    flex-shrink: 0;
    background: var(--surface);
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }}
  .detail-pane.collapsed {{ width: 0 !important; min-width: 0; }}
  .detail-pane.collapsed + .resizer-placeholder {{ display: none; }}
  .detail-header {{
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 12px;
    color: var(--text-muted);
    flex-shrink: 0;
  }}
  .detail-header span {{ color: var(--accent); font-weight: 600; }}
  .close-btn {{ background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: 16px; line-height: 1; padding: 2px 4px; }}
  .close-btn:hover {{ color: var(--text); }}
  .detail-body {{ display: flex; flex-direction: column; flex: 1; overflow: hidden; }}
  .state-panel {{
    flex-shrink: 0;
    overflow-y: auto;
    border-bottom: 1px solid var(--border);
    max-height: 55%;
    padding: 8px 0;
    font-size: 12px;
  }}

  /* Tree */
  .state-panel details {{ }}
  .state-panel summary {{
    list-style: none;
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 4px 14px;
    cursor: pointer;
    user-select: none;
    border-radius: 0;
  }}
  .state-panel summary::-webkit-details-marker {{ display: none; }}
  .state-panel summary:hover {{ background: color-mix(in srgb, var(--accent) 8%, transparent); }}
  .tree-arrow {{
    font-size: 9px;
    color: var(--text-muted);
    width: 10px;
    flex-shrink: 0;
    transition: transform 0.15s;
    display: inline-block;
  }}
  .state-panel details[open] > summary .tree-arrow {{ transform: rotate(90deg); }}
  .tree-label {{ flex: 1; color: var(--text); }}
  .tree-muted {{ color: var(--text-muted); font-size: 11px; }}
  .tree-badge {{
    font-size: 10px;
    padding: 1px 6px;
    border-radius: 999px;
    background: var(--surface2);
    color: var(--text-muted);
    flex-shrink: 0;
  }}
  .tree-children {{ padding-left: 18px; border-left: 1px solid var(--border); margin-left: 19px; }}
  .tree-tag {{
    font-family: var(--font-mono);
    font-size: 10px;
    padding: 1px 5px;
    border-radius: 3px;
    white-space: nowrap;
    flex-shrink: 0;
  }}
  .tag-worker {{ background: color-mix(in srgb, #10b981 20%, transparent); color: #10b981; }}
  .tag-running {{ background: color-mix(in srgb, #06b6d4 20%, transparent); color: #06b6d4; }}
  .tag-job {{ background: color-mix(in srgb, #3b82f6 20%, transparent); color: #3b82f6; }}
  .tree-detail-rows {{ padding: 3px 14px 4px 14px; }}
  .tree-detail-row {{
    display: flex;
    gap: 8px;
    padding: 1px 0;
    font-size: 11px;
    color: var(--text-muted);
  }}
  .tree-detail-key {{ color: var(--text-muted); min-width: 72px; flex-shrink: 0; }}
  .tree-detail-val {{ color: var(--text); font-family: var(--font-mono); font-size: 10px; }}
  .state-empty {{ color: var(--text-muted); font-style: italic; padding: 4px 14px; }}

  .json-panel {{ flex: 1; overflow: auto; }}
  pre.json {{
    font-family: var(--font-mono);
    font-size: 12px;
    line-height: 1.6;
    padding: 14px;
    white-space: pre-wrap;
    word-break: break-all;
    color: var(--text);
  }}

  .no-select {{ color: var(--text-muted); padding: 24px 14px; font-size: 13px; }}

  /* Category badge */
  .cat-badge {{
    font-family: var(--font-mono);
    font-size: 10px;
    font-weight: 700;
    padding: 1px 5px;
    border-radius: 3px;
    flex-shrink: 0;
    white-space: nowrap;
  }}

  /* Category legend popup */
  .cat-btn {{
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    color: var(--text);
    padding: 5px 10px;
    font-size: 12px;
    cursor: pointer;
    white-space: nowrap;
  }}
  .cat-btn:hover {{ border-color: var(--accent); }}
  .cat-btn.active-btn {{ border-color: var(--accent); color: var(--accent); }}
  .cat-popup-wrap {{ position: relative; }}
  .cat-popup {{
    display: none;
    position: absolute;
    top: calc(100% + 6px);
    left: 0;
    z-index: 100;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: 0 8px 24px rgba(0,0,0,0.5);
    min-width: 340px;
    padding: 8px 0;
  }}
  .cat-popup.open {{ display: block; }}
  .cat-popup-row {{
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 14px;
    font-size: 12px;
  }}
  .cat-popup-row:hover {{ background: color-mix(in srgb, var(--accent) 8%, transparent); }}
  .cat-popup-label {{ color: var(--text-muted); font-family: var(--font-mono); font-size: 11px; }}

  /* JSON syntax highlighting */
  .json-key {{ color: #93c5fd; }}
  .json-str {{ color: #86efac; }}
  .json-num {{ color: #fca5a5; }}
  .json-bool {{ color: #fbbf24; }}
  .json-null {{ color: #94a3b8; }}
</style>
</head>
<body>
<header>
  <h1>HyperQueue Event Viewer</h1>
  <span class="meta">Source: {html.escape(source_file)} &mdash; {len(events)} events{(" &mdash; " + _format_makespan(events)) if len(events) >= 2 else ""}</span>
</header>

<div class="chart-container">
  <div class="chart-canvas-wrap"><canvas id="chart"></canvas></div>
  <div class="chart-legend" id="chart-legend"></div>
</div>
<div class="chart-container" id="zoom-chart-container">
  <div class="chart-canvas-wrap">
    <canvas id="zoom-chart"></canvas>
    <div class="zoom-chart-header">
      <div class="zoom-range-wrap">
        <button class="zoom-range-btn" id="zoom-range-btn">1h zoom &#9660;</button>
        <div class="zoom-range-popup" id="zoom-range-popup"></div>
      </div>
      <span style="pointer-events:none">click to select event &nbsp;&bull;&nbsp; click overview to reposition</span>
    </div>
  </div>
</div>
<div class="chart-container" id="worker-chart-container" style="display:none">
  <div class="chart-canvas-wrap">
    <canvas id="worker-chart"></canvas>
    <div class="worker-chart-header"><span id="worker-chart-title"></span></div>
  </div>
  <div class="chart-legend" id="worker-chart-legend"></div>
</div>

<div class="toolbar">
  <input class="search" type="search" id="search" placeholder="Search events...">
  <button class="cat-btn" id="filter-btn">Filter by type</button>
  <button class="cat-btn active-btn" id="reltime-btn">Rel. time: on</button>
  <div class="cat-popup-wrap">
    <button class="cat-btn" id="worker-filter-btn">Filter by worker</button>
    <div class="cat-popup" id="worker-filter-popup"></div>
  </div>
  <div class="cat-popup-wrap">
    <button class="cat-btn" id="cat-btn">Task categories</button>
    <div class="cat-popup" id="cat-popup"></div>
  </div>
</div>

<div class="legend collapsed" id="legend">
{legend_html}
</div>

<div class="main">
  <div class="table-pane">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Time</th>
          <th>Type</th>
          <th>Summary</th>
        </tr>
      </thead>
      <tbody id="tbody">
{rows_html}
      </tbody>
    </table>
  </div>
  <div class="resizer" id="resizer"></div>
  <div class="detail-pane" id="detail-pane">
    <div class="detail-header">
      <span id="detail-title">Event detail</span>

    </div>
    <div class="detail-body" id="detail-body">
      <div class="state-panel" id="state-panel">
        <p class="no-select">Click an event to see details</p>
      </div>
      <div class="json-panel" id="json-panel"></div>
    </div>
  </div>
</div>

<script>
const JOB_CATEGORIES    = {job_to_cat_js};
const CATEGORY_INFO     = {cat_info_js};
const TIMELINE          = {timeline_js};
const WORKER_LIFETIMES  = {worker_lifetimes_js};
const TASK_TIMELINES    = {task_timelines_js};
const PENDING_TIMELINES  = {pending_timelines_js};
const ALLOC_QUEUED_TL   = {alloc_queued_js};

(function() {{
  const tbody = document.getElementById('tbody');
  const detailPane = document.getElementById('detail-pane');
  const detailTitle = document.getElementById('detail-title');
  const statePanel = document.getElementById('state-panel');
  const jsonPanel = document.getElementById('json-panel');
  const searchInput = document.getElementById('search');
  const legend = document.getElementById('legend');
  const resizer = document.getElementById('resizer');
  const main = detailPane.closest('.main');
  const catBtn = document.getElementById('cat-btn');
  const catPopup = document.getElementById('cat-popup');
  const filterBtn = document.getElementById('filter-btn');

  filterBtn.addEventListener('click', function() {{
    const collapsed = legend.classList.toggle('collapsed');
    filterBtn.textContent = collapsed ? 'Filter by type' : 'Hide filter';
  }});

  let showRelTime = true;
  const relTimeBtn = document.getElementById('reltime-btn');
  relTimeBtn.addEventListener('click', function() {{
    showRelTime = !showRelTime;
    relTimeBtn.textContent = 'Rel. time: ' + (showRelTime ? 'on' : 'off');
    relTimeBtn.classList.toggle('active-btn', showRelTime);
    updateRelTimes();
  }});

  let selectedRow = null;
  let activeFilters = new Set();

  // ── Category legend popup ─────────────────────────────────────────────
  (function buildCatPopup() {{
    let html = '';
    for (const [name, info] of Object.entries(CATEGORY_INFO)) {{
      html += '<div class="cat-popup-row">'
        + '<span class="cat-badge" style="background:' + info.color + ';color:#fff">' + name + '</span>'
        + '<span class="cat-popup-label">' + info.label + '</span>'
        + '</div>';
    }}
    catPopup.innerHTML = html || '<div style="padding:8px 14px;color:var(--text-muted)">No categories</div>';
  }})();

  catBtn.addEventListener('click', function(e) {{
    e.stopPropagation();
    catPopup.classList.toggle('open');
  }});
  document.addEventListener('click', function() {{
    catPopup.classList.remove('open');
    workerFilterPopup.classList.remove('open');
    zoomRangePopup.classList.remove('open');
  }});
  catPopup.addEventListener('click', function(e) {{ e.stopPropagation(); }});

  const workerFilterBtn   = document.getElementById('worker-filter-btn');
  const workerFilterPopup = document.getElementById('worker-filter-popup');

  workerFilterBtn.addEventListener('click', function(e) {{
    e.stopPropagation();
    if (selectedWorkerId !== null) {{ deselectWorker(); return; }}
    buildWorkerFilterPopup();
    workerFilterPopup.classList.toggle('open');
    if (workerFilterPopup.classList.contains('open')) {{
      setTimeout(function() {{
        const s = workerFilterPopup.querySelector('#worker-search');
        if (s) s.focus();
      }}, 0);
    }}
  }});
  workerFilterPopup.addEventListener('click', function(e) {{ e.stopPropagation(); }});

  // ── Parse all events from the DOM once ──────────────────────────────
  const allEvents = Array.from(
    tbody.querySelectorAll('.json-data')
  ).map(el => JSON.parse(el.textContent));

  // ── Incremental server-state simulation ─────────────────────────────
  // state.workers : Map<workerId, {{id, hostname}}>
  // state.jobs    : Map<jobId, {{name}}>
  // state.pending : Map<"j:t", {{jobId, taskId}}>
  // state.running : Map<"j:t", {{jobId, taskId, workerId}}>
  function emptyState() {{
    return {{ workers: new Map(), jobs: new Map(), pending: new Map(), running: new Map() }};
  }}

  function expandRanges(ranges) {{
    const ids = [];
    for (const r of ranges) {{
      for (let i = 0; i < r.count; i++) ids.push(r.start + i * r.step);
    }}
    return ids;
  }}

  function applyEventToState(st, event) {{
    const e = event.event;
    const type = e.type;
    if (type === 'worker-connected') {{
      st.workers.set(e.id, {{
        id: e.id,
        hostname: e.configuration?.hostname || '?',
        resources: parseWorkerResources(e.configuration?.resources?.resources),
      }});
    }} else if (type === 'worker-lost') {{
      st.workers.delete(e.id);
      // tasks on this worker remain running until task-failed clears them
    }} else if (type === 'job-created') {{
      const td = e.submit_desc?.task_desc || {{}};
      st.jobs.set(e.job, {{
        name: e.job_desc?.name || '',
        priority: td.priority,
        timeLimit: td.time_limit,
        crashLimit: td.crash_limit,
        variants: td.resources?.variants || [],
      }});
      const ranges = td.ids?.ranges || [];
      for (const tid of expandRanges(ranges)) {{
        st.pending.set(e.job + ':' + tid, {{ jobId: e.job, taskId: tid }});
      }}
    }} else if (type === 'task-started') {{
      const key = e.job + ':' + e.task;
      st.pending.delete(key);
      st.running.set(key, {{ jobId: e.job, taskId: e.task, workerId: e.worker, resources: jobResourcesMap[e.job] || {{}} }});
    }} else if (type === 'task-finished') {{
      const key = e.job + ':' + e.task;
      st.pending.delete(key);
      st.running.delete(key);
    }} else if (type === 'task-failed') {{
      const key = e.job + ':' + e.task;
      st.pending.delete(key);
      st.running.delete(key);
    }} else if (type === 'task-canceled') {{
      for (const t of (e.tasks || [])) {{
        const key = t.job_id + ':' + t.job_task_id;
        st.pending.delete(key);
        st.running.delete(key);
      }}
    }}
  }}

  // Cache: reuse forward progress when navigating forward
  let cachedIndex = -1;
  let cachedState = emptyState();

  function stateAt(targetIndex) {{
    if (targetIndex < cachedIndex) {{
      // Going backwards — replay from scratch
      cachedState = emptyState();
      cachedIndex = -1;
    }}
    for (let i = cachedIndex + 1; i <= targetIndex; i++) {{
      applyEventToState(cachedState, allEvents[i]);
    }}
    cachedIndex = targetIndex;
    return cachedState;
  }}

  // ── Job resource map ─────────────────────────────────────────────────
  // jobResourcesMap[jobId] = resName->amount from first variant
  const jobResourcesMap = {{}};
  for (const ev of allEvents) {{
    if (ev.event.type === 'job-created') {{
      const variants = ev.event.submit_desc?.task_desc?.resources?.variants || [];
      const res = {{}};
      for (const r of (variants[0]?.resources || []))
        res[r.resource] = r.policy ? Object.values(r.policy)[0] : 0;
      jobResourcesMap[ev.event.job] = res;
    }}
  }}

  // ── Worker filter popup ───────────────────────────────────────────────
  // Collect workers in connection order
  const allWorkers = [];
  for (const ev of allEvents) {{
    if (ev.event.type === 'worker-connected')
      allWorkers.push({{ id: ev.event.id, hostname: ev.event.configuration?.hostname || '?' }});
  }}

  function buildWorkerFilterPopup() {{
    if (!allWorkers.length) {{
      workerFilterPopup.innerHTML = '<div style="padding:8px 14px;color:var(--text-muted)">No workers</div>';
      return;
    }}
    let rows = '';
    for (const w of allWorkers) {{
      const sel = selectedWorkerId === w.id;
      rows += '<div class="cat-popup-row" data-worker-id="' + w.id + '" data-hostname="' + w.hostname + '" style="' + (sel ? 'background:color-mix(in srgb,var(--accent) 12%,transparent)' : '') + '">'
        + '<span class="tree-tag tag-worker' + (sel ? ' worker-selected' : '') + '">worker ' + w.id + '</span>'
        + '<span class="cat-popup-label">' + w.hostname + '</span>'
        + '</div>';
    }}
    workerFilterPopup.innerHTML =
      '<div style="padding:6px 8px;border-bottom:1px solid var(--border)">'
      + '<input id="worker-search" type="search" placeholder="Search workers\u2026" autocomplete="off"'
      + ' style="width:100%;background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);color:var(--text);padding:4px 8px;font-size:12px;outline:none">'
      + '</div>'
      + '<div id="worker-popup-rows">' + rows + '</div>';

    const workerSearch = workerFilterPopup.querySelector('#worker-search');
    const rowsContainer = workerFilterPopup.querySelector('#worker-popup-rows');
    workerSearch.addEventListener('input', function() {{
      const q = this.value.toLowerCase();
      for (const row of rowsContainer.querySelectorAll('.cat-popup-row')) {{
        const match = !q || String(row.dataset.workerId).includes(q) || row.dataset.hostname.toLowerCase().includes(q);
        row.style.display = match ? '' : 'none';
      }}
    }});
    workerSearch.addEventListener('click', function(e) {{ e.stopPropagation(); }});
  }}

  workerFilterPopup.addEventListener('click', function(e) {{
    const row = e.target.closest('[data-worker-id]');
    if (!row) return;
    const wid = parseInt(row.dataset.workerId);
    workerFilterPopup.classList.remove('open');
    if (selectedWorkerId === wid) deselectWorker(); else selectWorker(wid);
  }});

  // ── Helpers ──────────────────────────────────────────────────────────
  function esc(s) {{
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }}

  function catBadge(jobId) {{
    const name = JOB_CATEGORIES[jobId];
    if (!name) return '';
    const color = CATEGORY_INFO[name]?.color || '#888';
    return '<span class="cat-badge" style="background:' + color + ';color:#fff">' + name + '</span>';
  }}

  function fmtNum(v) {{
    // resource values are in units of 10_000
    const n = v / 10000;
    return n % 1 === 0 ? String(n) : n.toFixed(2).replace(/\\.?0+$/, '');
  }}

  function fmtSecs(secs) {{
    if (!secs) return '0s';
    const h = Math.floor(secs / 3600), m = Math.floor((secs % 3600) / 60), s = secs % 60;
    return [h && h+'h', m && m+'m', s && s+'s'].filter(Boolean).join(' ') || '0s';
  }}

  function fmtElapsed(ms) {{
    if (ms < 0) ms = 0;
    const h = Math.floor(ms / 3600000);
    const m = Math.floor((ms % 3600000) / 60000);
    const s = ((ms % 60000) / 1000).toFixed(2);
    if (h) return h + 'h ' + m + 'm ' + s + 's';
    if (m) return m + 'm ' + s + 's';
    return s + 's';
  }}

  function fmtDelta(ms) {{
    if (ms <= 0) return '+0ms';
    if (ms < 1000) return '+' + ms + 'ms';
    return '+' + fmtElapsed(ms);
  }}

  function parseWorkerResources(resList) {{
    const res = {{}};
    for (const r of (resList || [])) {{
      const kind = r.kind;
      if (kind && kind.Groups)
        res[r.name] = kind.Groups.groups.reduce((s, g) => s + g.length, 0) * 10000;
      else if (kind && kind.Sum)
        res[r.name] = kind.Sum.size;
    }}
    return res;
  }}

  const RES_COLORS = ['#93c5fd', '#6ee7b7', '#fca5a5', '#fcd34d', '#c4b5fd', '#fb923c'];
  const _resColorCache = {{}};
  let _resColorIdx = 0;
  function resColor(name) {{
    if (!_resColorCache[name]) _resColorCache[name] = RES_COLORS[_resColorIdx++ % RES_COLORS.length];
    return _resColorCache[name];
  }}

  function updateRelTimes() {{
    const originMs = (selectedWorkerId !== null && WORKER_LIFETIMES[selectedWorkerId])
      ? WORKER_LIFETIMES[selectedWorkerId][0]
      : eventMs[0];
    let prevVisibleMs = null;
    tbody.querySelectorAll('.event-row').forEach(function(row) {{
      const absSpan = row.querySelector('.abs-time');
      const relSpan = row.querySelector('.rel-time');
      if (!relSpan) return;
      const i = parseInt(row.dataset.index);
      const ms = eventMs[i];
      if (!showRelTime) {{
        if (absSpan) absSpan.style.display = '';
        relSpan.textContent = '';
      }} else {{
        if (absSpan) absSpan.style.display = 'none';
        const elapsed = ms - originMs;
        const delta = prevVisibleMs !== null ? ms - prevVisibleMs : 0;
        relSpan.textContent = fmtElapsed(elapsed) + ' (' + fmtDelta(delta) + ')';
      }}
      if (!row.classList.contains('hidden')) prevVisibleMs = ms;
    }});
  }}

  function fmtResVariants(variants) {{
    if (!variants || variants.length === 0) return 'none';
    return variants.map((v, vi) => {{
      const res = (v.resources || []).map(r => {{
        const amount = r.policy ? Object.values(r.policy)[0] : null;
        return esc(r.resource) + ': ' + (amount != null ? fmtNum(amount) : '?');
      }});
      const extras = [];
      if (v.min_time?.secs) extras.push('min_time: ' + fmtSecs(v.min_time.secs));
      if (v.n_nodes) extras.push('nodes: ' + v.n_nodes);
      const prefix = variants.length > 1 ? '<b>variant ' + vi + '</b> ' : '';
      return prefix + [...res, ...extras].join(', ');
    }}).join('<br>');
  }}

  function jobDetailRows(job) {{
    if (!job) return '';
    let h = '<div class="tree-detail-rows">';
    h += '<div class="tree-detail-row"><span class="tree-detail-key">resources</span>'
      + '<span class="tree-detail-val">' + fmtResVariants(job.variants) + '</span></div>';
    if (job.priority != null)
      h += '<div class="tree-detail-row"><span class="tree-detail-key">priority</span>'
        + '<span class="tree-detail-val">' + esc(job.priority) + '</span></div>';
    if (job.timeLimit)
      h += '<div class="tree-detail-row"><span class="tree-detail-key">time limit</span>'
        + '<span class="tree-detail-val">' + fmtSecs(job.timeLimit.secs) + '</span></div>';
    if (job.crashLimit)
      h += '<div class="tree-detail-row"><span class="tree-detail-key">on crash</span>'
        + '<span class="tree-detail-val">' + esc(job.crashLimit) + '</span></div>';
    h += '</div>';
    return h;
  }}

  function arrow() {{ return '<span class="tree-arrow">&#9658;</span>'; }}

  // ── Render server state as a tree ────────────────────────────────────
  function renderState(st) {{
    // Group running tasks by worker
    const byWorker = new Map();
    for (const [, t] of st.running) {{
      if (!byWorker.has(t.workerId)) byWorker.set(t.workerId, []);
      byWorker.get(t.workerId).push(t);
    }}
    // Group pending tasks by job
    const pendingByJob = new Map();
    for (const [, t] of st.pending) {{
      if (!pendingByJob.has(t.jobId)) pendingByJob.set(t.jobId, []);
      pendingByJob.get(t.jobId).push(t);
    }}

    let h = '<div>';

    function catSummaryHtml(tasks) {{
      const byCat = new Map();
      for (const t of tasks) {{
        const cat = JOB_CATEGORIES[t.jobId];
        if (cat) byCat.set(cat, (byCat.get(cat) || 0) + 1);
      }}
      return Array.from(byCat.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([cat, n]) => {{
          const color = CATEGORY_INFO[cat]?.color || '#888';
          return n + 'x <span class="cat-badge" style="background:' + color + ';color:#fff">' + cat + '</span>';
        }}).join(' &nbsp; ');
    }}

    // ── Active workers root ──
    const allRunning = Array.from(st.running.values());
    const rootRunSummary = catSummaryHtml(allRunning);
    h += '<details open><summary>'
      + arrow()
      + '<span class="tree-label">Active workers &nbsp; ' + rootRunSummary + '</span>'
      + '<span class="tree-badge">' + st.workers.size + '</span>'
      + '</summary><div class="tree-children">';
    if (st.workers.size === 0) {{
      h += '<span class="state-empty">none</span>';
    }} else {{
      for (const [, w] of st.workers) {{
        const wTasks = byWorker.get(w.id) || [];
        const nTasks = wTasks.length;
        const wSummary = catSummaryHtml(wTasks);
        // resource utilization rows
        const wTotals = w.resources || {{}};
        const wUsed = {{}};
        for (const t of wTasks)
          for (const [res, amt] of Object.entries(t.resources || {{}}))
            wUsed[res] = (wUsed[res] || 0) + amt;
        const resRows = Object.entries(wTotals).map(([res, total]) => {{
          const u = wUsed[res] || 0;
          const pct = total > 0 ? Math.round(u / total * 100) : 0;
          return '<div class="tree-detail-row"><span class="tree-detail-key">' + esc(res) + '</span>'
            + '<span class="tree-detail-val">' + fmtNum(u) + ' / ' + fmtNum(total) + ' (' + pct + '%)</span></div>';
        }}).join('');

        h += '<details><summary>'
          + arrow()
          + '<span class="tree-tag tag-worker' + (selectedWorkerId === w.id ? ' worker-selected' : '') + '" data-worker-id="' + w.id + '">worker ' + esc(w.id) + '</span>'
          + '<span class="tree-label">' + esc(w.hostname) + ' &nbsp; ' + wSummary + '</span>'
          + '<span class="tree-muted">' + nTasks + ' running</span>'
          + '</summary><div class="tree-children">';
        if (resRows) h += '<div class="tree-detail-rows">' + resRows + '</div>';
        if (nTasks === 0) {{
          h += '<span class="state-empty">idle</span>';
        }} else {{
          for (const t of wTasks) {{
            const job = st.jobs.get(t.jobId);
            const jobName = job?.name ? ' ' + esc(job.name) : '';
            h += '<details><summary>'
              + arrow()
              + '<span class="tree-tag tag-running">' + esc(t.jobId) + '@' + esc(t.taskId) + '</span>'
              + catBadge(t.jobId)
              + (jobName ? '<span class="tree-muted">' + jobName.trim() + '</span>' : '')
              + '</summary>'
              + jobDetailRows(job)
              + '</details>';
          }}
        }}
        h += '</div></details>';
      }}
    }}
    h += '</div></details>';

    // ── Pending tasks root ──
    const pendingByCat = new Map();
    for (const [, t] of st.pending) {{
      const cat = JOB_CATEGORIES[t.jobId];
      if (cat) pendingByCat.set(cat, (pendingByCat.get(cat) || 0) + 1);
    }}
    const catSummary = Array.from(pendingByCat.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([cat, n]) => {{
        const color = CATEGORY_INFO[cat]?.color || '#888';
        return n + 'x <span class="cat-badge" style="background:' + color + ';color:#fff">' + cat + '</span>';
      }}).join(' &nbsp; ');
    h += '<details><summary>'
      + arrow()
      + '<span class="tree-label">Pending tasks &nbsp; ' + catSummary + '</span>'
      + '<span class="tree-badge">' + st.pending.size + '</span>'
      + '</summary><div class="tree-children">';
    if (pendingByJob.size === 0) {{
      h += '<span class="state-empty">none</span>';
    }} else {{
      for (const [jobId, tasks] of pendingByJob) {{
        const job = st.jobs.get(jobId);
        const jobName = job?.name ? ' ' + esc(job.name) : '';
        const n = tasks.length;
        h += '<details><summary>'
          + arrow()
          + '<span class="tree-tag tag-job">job ' + esc(jobId) + '</span>'
          + catBadge(jobId)
          + '<span class="tree-label">' + jobName + '</span>'
          + '<span class="tree-muted">' + n + ' task' + (n !== 1 ? 's' : '') + '</span>'
          + '</summary><div class="tree-children">';
        for (const t of tasks) {{
          h += '<details><summary>'
            + arrow()
            + '<span class="tree-tag tag-job">' + esc(t.jobId) + '@' + esc(t.taskId) + '</span>'
            + catBadge(t.jobId)
            + '</summary>'
            + jobDetailRows(job)
            + '</details>';
        }}
        h += '</div></details>';
      }}
    }}
    h += '</div></details>';

    h += '</div>';
    return h;
  }}

  // ── Syntax-highlight JSON ────────────────────────────────────────────
  function syntaxHighlight(json) {{
    return json
      .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
      .replace(/("(\\u[a-fA-F0-9]{{4}}|\\[^u]|[^\\"])*"(\\s*:)?|\\b(true|false|null)\\b|-?\\d+\\\\.?\\d*([eE][+-]?\\d+)?)/g, function(match) {{
        let cls = 'json-num';
        if (/^"/.test(match)) {{
          cls = /:$/.test(match) ? 'json-key' : 'json-str';
        }} else if (/true|false/.test(match)) {{
          cls = 'json-bool';
        }} else if (/null/.test(match)) {{
          cls = 'json-null';
        }}
        return '<span class="' + cls + '">' + match.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</span>';
      }});
  }}

  // ── Row selection ────────────────────────────────────────────────────
  function selectRow(row) {{
    if (selectedRow) selectedRow.classList.remove('selected');
    selectedRow = row;
    row.classList.add('selected');

    const idx = parseInt(row.dataset.index);
    const etype = row.dataset.type;
    const jsonText = row.querySelector('.json-data').textContent;

    detailTitle.textContent = '#' + (idx + 1) + ' \u2014 ' + etype;
    jsonPanel.innerHTML = '<pre class="json">' + syntaxHighlight(jsonText) + '</pre>';

    const st = stateAt(idx);
    statePanel.innerHTML = renderState(st);

    const wid = (etype === 'worker-connected' || etype === 'worker-lost') ? allEvents[idx].event.id : null;

    // Move zoom window if the selected event falls outside it
    if (zoomCenterMs !== null && eventMs.length > 0) {{
      const ms = eventMs[idx];
      const half = ZOOM_WINDOW_MS / 2;
      if (ms < zoomCenterMs - half || ms > zoomCenterMs + half) {{
        const totalMin = eventMs[0], totalMax = eventMs[eventMs.length - 1];
        zoomCenterMs = Math.max(totalMin + half, Math.min(totalMax - half, ms));
      }}
    }}

    refreshChart(idx, wid);
  }}

  function closeDetail() {{
    if (selectedRow) {{ selectedRow.classList.remove('selected'); selectedRow = null; }}
    detailTitle.textContent = 'Event detail';
    statePanel.innerHTML = '<p class="no-select">Click an event to see details</p>';
    jsonPanel.innerHTML = '';
    refreshChart(null, null);
  }}

  tbody.addEventListener('click', function(e) {{
    const row = e.target.closest('.event-row');
    if (!row) return;
    if (row === selectedRow) {{ closeDetail(); return; }}
    selectRow(row);
  }});


  statePanel.addEventListener('click', function(e) {{
    const tag = e.target.closest('.tag-worker[data-worker-id]');
    if (!tag) return;
    e.stopPropagation();
    selectWorker(parseInt(tag.dataset.workerId));
  }});

  // ── Filtering ────────────────────────────────────────────────────────
  function applyFilters() {{
    const query = searchInput.value.toLowerCase();
    const rows = tbody.querySelectorAll('.event-row');
    rows.forEach(function(row) {{
      const typeMatch = activeFilters.size === 0 || activeFilters.has(row.dataset.type);
      const text = row.textContent.toLowerCase();
      const textMatch = !query || text.includes(query);
      const workerMatch = !workerFilterIndices || workerFilterIndices.has(parseInt(row.dataset.index));
      row.classList.toggle('hidden', !(typeMatch && textMatch && workerMatch));
    }});
    if (selectedRow && !selectedRow.classList.contains('hidden')) {{
      selectedRow.scrollIntoView({{block: 'nearest'}});
    }}
    updateRelTimes();
  }}

  searchInput.addEventListener('input', applyFilters);

  legend.addEventListener('click', function(e) {{
    const item = e.target.closest('.legend-item');
    if (!item) return;
    const type = item.dataset.type;
    if (activeFilters.has(type)) {{
      activeFilters.delete(type);
      item.classList.remove('active');
    }} else {{
      activeFilters.add(type);
      item.classList.add('active');
    }}
    applyFilters();
  }});

  // ── Resizer drag ─────────────────────────────────────────────────────
  resizer.addEventListener('mousedown', function(e) {{
    e.preventDefault();
    resizer.classList.add('dragging');
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    function onMouseMove(e) {{
      const mainRect = main.getBoundingClientRect();
      const newDetailWidth = mainRect.right - e.clientX;
      const minW = 150;
      const maxW = mainRect.width - 200 - 5;
      detailPane.style.width = Math.min(maxW, Math.max(minW, newDetailWidth)) + 'px';
    }}

    function onMouseUp() {{
      resizer.classList.remove('dragging');
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    }}

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }});

  // ── Chart ────────────────────────────────────────────────────────────
  const canvas = document.getElementById('chart');
  const ctx = canvas.getContext('2d');
  const chartLegendEl = document.getElementById('chart-legend');
  let chartEventIndex = null;
  let chartWorkerId   = null;
  const hiddenLines   = new Set();

  const ZOOM_RANGES = [
    {{ label: '5min',  ms: 5 * 60 * 1000 }},
    {{ label: '30min', ms: 30 * 60 * 1000 }},
    {{ label: '1h',    ms: 60 * 60 * 1000 }},
    {{ label: '2h',    ms: 2 * 60 * 60 * 1000 }},
    {{ label: '5h',    ms: 5 * 60 * 60 * 1000 }},
  ];
  let ZOOM_WINDOW_MS = 3600000;
  let zoomCenterMs = null;
  const zoomCanvas = document.getElementById('zoom-chart');
  const zoomCtx = zoomCanvas.getContext('2d');
  const ZPL = 8, ZPR = 8, ZPT = 24, ZPB = 18;

  // ── Zoom range dropdown ───────────────────────────────────────────────
  const zoomRangeBtn   = document.getElementById('zoom-range-btn');
  const zoomRangePopup = document.getElementById('zoom-range-popup');

  function buildZoomRangePopup() {{
    zoomRangePopup.innerHTML = ZOOM_RANGES.map(r =>
      '<div class="zoom-range-option' + (r.ms === ZOOM_WINDOW_MS ? ' selected' : '') + '" data-ms="' + r.ms + '">' + r.label + '</div>'
    ).join('');
  }}

  zoomRangeBtn.addEventListener('click', function(e) {{
    e.stopPropagation();
    buildZoomRangePopup();
    zoomRangePopup.classList.toggle('open');
  }});

  zoomRangePopup.addEventListener('click', function(e) {{
    const opt = e.target.closest('.zoom-range-option');
    if (!opt) return;
    ZOOM_WINDOW_MS = parseInt(opt.dataset.ms);
    const label = ZOOM_RANGES.find(r => r.ms === ZOOM_WINDOW_MS)?.label || '';
    zoomRangeBtn.textContent = label + ' zoom \u25bc';
    zoomRangePopup.classList.remove('open');
    // Re-clamp zoom center for new window size
    if (zoomCenterMs !== null && eventMs.length > 0) {{
      const half = ZOOM_WINDOW_MS / 2;
      zoomCenterMs = Math.max(eventMs[0] + half, Math.min(eventMs[eventMs.length - 1] - half, zoomCenterMs));
    }}
    drawChart();
    drawZoomChart();
  }});

  zoomRangePopup.addEventListener('click', function(e) {{ e.stopPropagation(); }});

  const workerChartCanvas    = document.getElementById('worker-chart');
  const workerCtx            = workerChartCanvas.getContext('2d');
  const workerChartLegendEl  = document.getElementById('worker-chart-legend');
  const workerChartContainer = document.getElementById('worker-chart-container');
  const workerChartTitle     = document.getElementById('worker-chart-title');
  let selectedWorkerId     = null;
  let workerFilterIndices  = null;
  let workerTaskTimelines  = {{}};
  let workerResourceData   = {{ totals: {{}}, timelines: {{}} }};
  const workerChartHiddenLines = new Set();
  const WC_PL = 8, WC_PR = 8, WC_PT = 26, WC_PB = 4;

  function computeWorkerTaskTimeline(workerId) {{
    const running = {{}};
    const timelines = {{}};
    const workerTasks = {{}};
    const first_ms = eventMs[0], last_ms = eventMs[eventMs.length - 1];
    for (let i = 0; i < allEvents.length; i++) {{
      const e = allEvents[i].event;
      const ms = eventMs[i];
      if (e.type === 'task-started' && e.worker == workerId) {{
        const cat = JOB_CATEGORIES[e.job];
        if (cat) {{
          workerTasks[e.job + ':' + e.task] = cat;
          const old = running[cat] || 0;
          const tl = timelines[cat] || (timelines[cat] = []);
          tl.push([ms, old]);
          tl.push([ms, old + 1]);
          running[cat] = old + 1;
        }}
      }} else if (e.type === 'task-finished' || e.type === 'task-failed') {{
        const key = e.job + ':' + e.task;
        const cat = workerTasks[key];
        if (cat) {{
          delete workerTasks[key];
          const old = running[cat] || 0;
          const tl = timelines[cat];
          if (tl) {{ tl.push([ms, old]); tl.push([ms, Math.max(0, old - 1)]); }}
          running[cat] = Math.max(0, old - 1);
        }}
      }} else if (e.type === 'task-canceled') {{
        for (const t of (e.tasks || [])) {{
          const key = t.job_id + ':' + t.job_task_id;
          const cat = workerTasks[key];
          if (cat) {{
            delete workerTasks[key];
            const old = running[cat] || 0;
            const tl = timelines[cat];
            if (tl) {{ tl.push([ms, old]); tl.push([ms, Math.max(0, old - 1)]); }}
            running[cat] = Math.max(0, old - 1);
          }}
        }}
      }}
    }}
    for (const [cat, tl] of Object.entries(timelines)) {{
      if (tl[0][0] > first_ms) tl.unshift([first_ms, 0]);
      if (tl[tl.length-1][0] < last_ms) tl.push([last_ms, running[cat] || 0]);
    }}
    return timelines;
  }}

  function computeWorkerResourceTimelines(workerId) {{
    let totals = {{}};
    for (const ev of allEvents)
      if (ev.event.type === 'worker-connected' && ev.event.id == workerId) {{
        totals = parseWorkerResources(ev.event.configuration?.resources?.resources);
        break;
      }}
    const used = {{}};
    const timelines = {{}};
    const taskResMap = {{}};
    const first_ms = eventMs[0], last_ms = eventMs[eventMs.length - 1];
    for (let i = 0; i < allEvents.length; i++) {{
      const e = allEvents[i].event, ms = eventMs[i];
      function applyDelta(key, sign) {{
        const jobRes = taskResMap[key];
        if (!jobRes) return;
        if (sign < 0) delete taskResMap[key];
        for (const [res, amt] of Object.entries(jobRes)) {{
          const old = used[res] || 0;
          const tl = timelines[res] || (timelines[res] = []);
          tl.push([ms, old]);
          tl.push([ms, Math.max(0, old + sign * amt)]);
          used[res] = Math.max(0, old + sign * amt);
        }}
      }}
      if (e.type === 'task-started' && e.worker == workerId) {{
        const jobRes = jobResourcesMap[e.job] || {{}};
        taskResMap[e.job + ':' + e.task] = jobRes;
        applyDelta(e.job + ':' + e.task, 1);
      }} else if (e.type === 'task-finished' || e.type === 'task-failed') {{
        applyDelta(e.job + ':' + e.task, -1);
      }} else if (e.type === 'task-canceled') {{
        for (const t of (e.tasks || [])) applyDelta(t.job_id + ':' + t.job_task_id, -1);
      }}
    }}
    for (const [res, tl] of Object.entries(timelines)) {{
      if (tl[0][0] > first_ms) tl.unshift([first_ms, 0]);
      if (tl[tl.length-1][0] < last_ms) tl.push([last_ms, used[res] || 0]);
    }}
    return {{ totals, timelines }};
  }}

  function getWorkerRelatedIndices(workerId) {{
    const indices = new Set();
    const workerTasks = new Set();
    for (let i = 0; i < allEvents.length; i++) {{
      const e = allEvents[i].event;
      if ((e.type === 'worker-connected' || e.type === 'worker-lost') && e.id == workerId) {{
        indices.add(i);
      }} else if (e.type === 'task-started' && e.worker == workerId) {{
        indices.add(i);
        workerTasks.add(e.job + ':' + e.task);
      }} else if (e.type === 'task-finished' || e.type === 'task-failed') {{
        const key = e.job + ':' + e.task;
        if (workerTasks.has(key)) {{ indices.add(i); workerTasks.delete(key); }}
      }} else if (e.type === 'task-canceled') {{
        for (const t of (e.tasks || [])) {{
          const key = t.job_id + ':' + t.job_task_id;
          if (workerTasks.has(key)) {{ indices.add(i); workerTasks.delete(key); }}
        }}
      }}
    }}
    return indices;
  }}

  function setupWorkerCanvas() {{
    const dpr = window.devicePixelRatio || 1;
    const w = workerChartCanvas.parentElement.offsetWidth;
    const h = workerChartCanvas.parentElement.offsetHeight;
    workerChartCanvas.width  = w * dpr;
    workerChartCanvas.height = h * dpr;
    workerCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }}

  function drawWorkerChart() {{
    const W = workerChartCanvas.parentElement.offsetWidth;
    const H = workerChartCanvas.parentElement.offsetHeight;
    workerCtx.clearRect(0, 0, W, H);
    if (TIMELINE.length < 2 || !Object.keys(workerTaskTimelines).length) return;
    const lt = WORKER_LIFETIMES[selectedWorkerId];
    if (!lt) return;
    const minMs = lt[0];
    const maxMs = lt[1] !== null ? lt[1] : TIMELINE[TIMELINE.length - 1][0];
    const span  = maxMs - minMs || 1;
    const g = {{ W, H, PL: WC_PL, PR: WC_PR, PT: WC_PT, PB: WC_PB, minMs, span }};
    const CW = W - WC_PL - WC_PR, CH = H - WC_PT - WC_PB;
    function toX(ms) {{ return WC_PL + (ms - minMs) / span * CW; }}
    workerCtx.strokeStyle = 'rgba(255,255,255,0.05)';
    workerCtx.lineWidth = 1;
    for (const f of [0.25, 0.5, 0.75, 1.0]) {{
      const y = WC_PT + CH - f * CH;
      workerCtx.beginPath(); workerCtx.moveTo(WC_PL, y); workerCtx.lineTo(W - WC_PR, y); workerCtx.stroke();
    }}
    for (const [cat, tl] of Object.entries(workerTaskTimelines)) {{
      if (workerChartHiddenLines.has(cat)) continue;
      const color = CATEGORY_INFO[cat]?.color || '#888';
      const maxVal = Math.max(...tl.map(p => p[1]), 1);
      drawStepLine(workerCtx, g, tl, maxVal, color, 0.12, false);
    }}
    for (const [res, tl] of Object.entries(workerResourceData.timelines)) {{
      if (workerChartHiddenLines.has('res-' + res)) continue;
      const total = workerResourceData.totals[res] || 1;
      const color = resColor(res);
      drawStepLine(workerCtx, g, tl, total, color, 0.08, true);
    }}
    if (chartEventIndex !== null) {{
      const ms = new Date(allEvents[chartEventIndex].time).getTime();
      if (ms >= minMs && ms <= maxMs) {{
        const x = toX(ms);
        workerCtx.beginPath(); workerCtx.moveTo(x, WC_PT); workerCtx.lineTo(x, WC_PT + CH);
        workerCtx.strokeStyle = '#fbbf24';
        workerCtx.lineWidth = 1.5;
        workerCtx.setLineDash([4, 3]);
        workerCtx.stroke();
        workerCtx.setLineDash([]);
      }}
    }}
    workerCtx.fillStyle = '#475569';
    workerCtx.font = '10px monospace';
    workerCtx.textAlign = 'left';
    workerCtx.fillText(new Date(minMs).toISOString().slice(0, 23).replace('T', ' '), WC_PL, H - 2);
    workerCtx.textAlign = 'right';
    workerCtx.fillText(new Date(maxMs).toISOString().slice(0, 23).replace('T', ' '), W - WC_PR, H - 2);
  }}

  function updateWorkerChartLegend() {{
    let html = '';
    const hiddenCls = k => workerChartHiddenLines.has(k) ? ' hidden-line' : '';
    for (const [cat, tl] of Object.entries(workerTaskTimelines)) {{
      const color = CATEGORY_INFO[cat]?.color || '#888';
      const maxV = Math.max(...tl.map(p => p[1]));
      html += '<span class="chart-legend-item' + hiddenCls(cat) + '" data-line="' + cat + '">'
        + '<span class="chart-legend-line" style="background:' + color + '"></span>'
        + cat + ': max ' + maxV + ' tasks</span>';
    }}
    for (const [res, tl] of Object.entries(workerResourceData.timelines)) {{
      const total = workerResourceData.totals[res] || 1;
      const color = resColor(res);
      const maxV = Math.max(...tl.map(p => p[1]));
      const maxPct = Math.round(maxV / total * 100);
      html += '<span class="chart-legend-item' + hiddenCls('res-' + res) + '" data-line="res-' + res + '">'
        + '<span class="chart-legend-line" style="background:' + color + ';opacity:0.6"></span>'
        + res + ': max ' + fmtNum(maxV) + '/' + fmtNum(total) + ' (' + maxPct + '%)</span>';
    }}
    workerChartLegendEl.innerHTML = html;
  }}

  function selectWorker(wid) {{
    selectedWorkerId = wid;
    workerFilterIndices = getWorkerRelatedIndices(wid);
    workerTaskTimelines = computeWorkerTaskTimeline(wid);
    workerResourceData  = computeWorkerResourceTimelines(wid);
    workerChartContainer.style.display = '';
    workerChartTitle.textContent = 'Worker ' + wid + ' detail';
    workerFilterBtn.textContent = 'Worker ' + wid + ' \u00d7';
    workerFilterBtn.style.borderColor = 'var(--accent)';
    buildWorkerFilterPopup();
    setupWorkerCanvas();
    drawWorkerChart();
    updateWorkerChartLegend();
    applyFilters();
    updateRelTimes();
    if (selectedRow) statePanel.innerHTML = renderState(stateAt(parseInt(selectedRow.dataset.index)));
    const selMs = chartEventIndex !== null ? new Date(allEvents[chartEventIndex].time).getTime() : null;
    drawChart(); updateChartLegend(selMs);
  }}

  function deselectWorker() {{
    selectedWorkerId = null;
    workerFilterIndices = null;
    workerTaskTimelines = {{}};
    workerResourceData  = {{ totals: {{}}, timelines: {{}} }};
    workerChartContainer.style.display = 'none';
    workerFilterBtn.textContent = 'Filter by worker';
    workerFilterBtn.style.borderColor = '';
    buildWorkerFilterPopup();
    applyFilters();
    updateRelTimes();
    if (selectedRow) statePanel.innerHTML = renderState(stateAt(parseInt(selectedRow.dataset.index)));
    const etype = selectedRow ? selectedRow.dataset.type : null;
    const wid = (etype === 'worker-connected' || etype === 'worker-lost')
      ? allEvents[parseInt(selectedRow.dataset.index)].event.id : null;
    refreshChart(chartEventIndex, wid);
  }}

  workerChartLegendEl.addEventListener('click', function(e) {{
    const item = e.target.closest('.chart-legend-item');
    if (!item) return;
    const key = item.dataset.line;
    if (workerChartHiddenLines.has(key)) workerChartHiddenLines.delete(key);
    else workerChartHiddenLines.add(key);
    item.classList.toggle('hidden-line', workerChartHiddenLines.has(key));
    drawWorkerChart();
  }});

  chartLegendEl.addEventListener('click', function(e) {{
    const item = e.target.closest('.chart-legend-item');
    if (!item) return;
    const key = item.dataset.line;
    if (hiddenLines.has(key)) hiddenLines.delete(key);
    else hiddenLines.add(key);
    item.classList.toggle('hidden-line', hiddenLines.has(key));
    drawChart();
    drawZoomChart();
  }});

  const PL = 8, PR = 8, PT = 8, PB = 18;

  function setupCanvas() {{
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.parentElement.offsetWidth;
    const h = canvas.parentElement.offsetHeight;
    canvas.width  = w * dpr;
    canvas.height = h * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }}

  function valueAt(tl, ms) {{
    let v = 0;
    for (const [t, c] of tl) {{ if (t > ms) break; v = c; }}
    return v;
  }}

  function drawStepLine(cctx, g, tl, maxVal, color, fillAlpha, dashed) {{
    if (!tl || tl.length < 2) return;
    const CW = g.W - g.PL - g.PR, CH = g.H - g.PT - g.PB;
    function toX(ms) {{ return g.PL + (ms - g.minMs) / g.span * CW; }}
    function toY(v)  {{ return g.PT + CH - (v / maxVal) * CH; }}

    if (dashed) cctx.setLineDash([4, 3]);
    cctx.beginPath();
    cctx.moveTo(toX(tl[0][0]), toY(tl[0][1]));
    for (const [ms, c] of tl) cctx.lineTo(toX(ms), toY(c));
    cctx.strokeStyle = color;
    cctx.lineWidth = dashed ? 1 : 1.5;
    cctx.stroke();
    if (dashed) cctx.setLineDash([]);
  }}

  function setupZoomCanvas() {{
    const dpr = window.devicePixelRatio || 1;
    const w = zoomCanvas.parentElement.offsetWidth;
    const h = zoomCanvas.parentElement.offsetHeight;
    zoomCanvas.width  = w * dpr;
    zoomCanvas.height = h * dpr;
    zoomCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }}

  function drawZoomChart() {{
    const W = zoomCanvas.parentElement.offsetWidth;
    const H = zoomCanvas.parentElement.offsetHeight;
    const CW = W - ZPL - ZPR, CH = H - ZPT - ZPB;
    zoomCtx.clearRect(0, 0, W, H);
    if (TIMELINE.length < 2 || zoomCenterMs === null) return;

    const half = ZOOM_WINDOW_MS / 2;
    const minMs = zoomCenterMs - half;
    const span  = ZOOM_WINDOW_MS;
    function toX(ms) {{ return ZPL + (ms - minMs) / span * CW; }}
    function toY(v, maxVal) {{ return ZPT + CH - (v / maxVal) * CH; }}

    // Grid
    zoomCtx.strokeStyle = 'rgba(255,255,255,0.05)';
    zoomCtx.lineWidth = 1;
    for (const f of [0.25, 0.5, 0.75, 1.0]) {{
      const y = ZPT + CH - f * CH;
      zoomCtx.beginPath(); zoomCtx.moveTo(ZPL, y); zoomCtx.lineTo(W - ZPR, y); zoomCtx.stroke();
    }}

    // Clip all line drawing to the data area
    zoomCtx.save();
    zoomCtx.beginPath();
    zoomCtx.rect(ZPL, ZPT, CW, CH + 1);
    zoomCtx.clip();

    const g = {{ W, H, PL: ZPL, PR: ZPR, PT: ZPT, PB: ZPB, minMs, span }};

    for (const [cat, tl] of Object.entries(PENDING_TIMELINES)) {{
      if (hiddenLines.has(cat + '-pend')) continue;
      const color = CATEGORY_INFO[cat]?.color || '#888';
      const maxVal = Math.max(...tl.map(p => p[1]), 1);
      drawStepLine(zoomCtx, g, tl, maxVal, color, 0, true);
    }}
    for (const [cat, tl] of Object.entries(TASK_TIMELINES)) {{
      if (hiddenLines.has(cat + '-run')) continue;
      const color = CATEGORY_INFO[cat]?.color || '#888';
      const maxVal = Math.max(...tl.map(p => p[1]), 1);
      drawStepLine(zoomCtx, g, tl, maxVal, color, 0.10, false);
    }}
    if (!hiddenLines.has('workers')) {{
      const maxW = Math.max(...TIMELINE.map(p => p[1]), 1);
      drawStepLine(zoomCtx, g, TIMELINE, maxW, '#38bdf8', 0.08, false);
    }}
    if (!hiddenLines.has('alloc-queued') && ALLOC_QUEUED_TL.length >= 2) {{
      const maxV = Math.max(...ALLOC_QUEUED_TL.map(p => p[1]), 1);
      drawStepLine(zoomCtx, g, ALLOC_QUEUED_TL, maxV, '#a3e635', 0, true);
    }}
    // Event tick marks along the bottom
    const winMax = zoomCenterMs + half;
    let tickCount = 0;
    for (let i = 0; i < eventMs.length; i++) {{
      if (eventMs[i] >= minMs && eventMs[i] <= winMax) tickCount++;
    }}
    if (tickCount <= 600) {{
      zoomCtx.strokeStyle = 'rgba(255,255,255,0.25)';
      zoomCtx.lineWidth = 1;
      for (let i = 0; i < eventMs.length; i++) {{
        if (eventMs[i] < minMs || eventMs[i] > winMax) continue;
        const x = toX(eventMs[i]);
        zoomCtx.beginPath(); zoomCtx.moveTo(x, ZPT + CH - 5); zoomCtx.lineTo(x, ZPT + CH); zoomCtx.stroke();
      }}
    }}

    zoomCtx.restore();

    // Selected-event vertical line
    if (chartEventIndex !== null) {{
      const ms = new Date(allEvents[chartEventIndex].time).getTime();
      if (ms >= minMs && ms <= winMax) {{
        const x = toX(ms);
        zoomCtx.beginPath(); zoomCtx.moveTo(x, ZPT); zoomCtx.lineTo(x, ZPT + CH);
        zoomCtx.strokeStyle = '#fbbf24';
        zoomCtx.lineWidth = 1.5;
        zoomCtx.setLineDash([4, 3]);
        zoomCtx.stroke();
        zoomCtx.setLineDash([]);
      }}
    }}

    // X axis time labels
    zoomCtx.fillStyle = '#475569';
    zoomCtx.font = '10px monospace';
    zoomCtx.textAlign = 'left';
    zoomCtx.fillText(new Date(minMs).toISOString().slice(0, 23).replace('T', ' '), ZPL, H - 2);
    zoomCtx.textAlign = 'right';
    zoomCtx.fillText(new Date(zoomCenterMs + half).toISOString().slice(0, 23).replace('T', ' '), W - ZPR, H - 2);
  }}

  function drawChart() {{
    const W = canvas.parentElement.offsetWidth;
    const H = canvas.parentElement.offsetHeight;
    const CW = W - PL - PR, CH = H - PT - PB;

    ctx.clearRect(0, 0, W, H);
    if (TIMELINE.length < 2) return;

    const minMs = TIMELINE[0][0];
    const maxMs = TIMELINE[TIMELINE.length - 1][0];
    const span  = maxMs - minMs || 1;
    function toX(ms) {{ return PL + (ms - minMs) / span * CW; }}

    // Subtle horizontal grid lines at 25/50/75/100%
    ctx.strokeStyle = 'rgba(255,255,255,0.05)';
    ctx.lineWidth = 1;
    for (const f of [0.25, 0.5, 0.75, 1.0]) {{
      const y = PT + CH - f * CH;
      ctx.beginPath(); ctx.moveTo(PL, y); ctx.lineTo(W - PR, y); ctx.stroke();
    }}

    // Worker lifetime highlight
    const highlightWid = selectedWorkerId !== null ? selectedWorkerId : chartWorkerId;
    if (highlightWid !== null) {{
      const lt = WORKER_LIFETIMES[highlightWid];
      if (lt) {{
        const x1 = toX(lt[0]);
        const x2 = lt[1] !== null ? toX(lt[1]) : toX(maxMs);
        ctx.fillStyle = 'rgba(56,189,248,0.30)';
        ctx.fillRect(x1, PT, x2 - x1, CH);
        ctx.strokeStyle = 'rgba(56,189,248,0.90)';
        ctx.lineWidth = 2;
        ctx.setLineDash([4, 3]);
        for (const xv of [x1, ...(lt[1] !== null ? [x2] : [])]) {{
          ctx.beginPath(); ctx.moveTo(xv, PT); ctx.lineTo(xv, PT + CH); ctx.stroke();
        }}
        ctx.setLineDash([]);
      }}
    }}

    const g = {{ W, H, PL, PR, PT, PB, minMs, span }};

    // Pending lines (dashed, drawn first)
    for (const [cat, tl] of Object.entries(PENDING_TIMELINES)) {{
      if (hiddenLines.has(cat + '-pend')) continue;
      const color = CATEGORY_INFO[cat]?.color || '#888';
      const maxVal = Math.max(...tl.map(p => p[1]), 1);
      drawStepLine(ctx, g, tl, maxVal, color, 0, true);
    }}

    // Running lines (solid, on top of pending)
    for (const [cat, tl] of Object.entries(TASK_TIMELINES)) {{
      if (hiddenLines.has(cat + '-run')) continue;
      const color = CATEGORY_INFO[cat]?.color || '#888';
      const maxVal = Math.max(...tl.map(p => p[1]), 1);
      drawStepLine(ctx, g, tl, maxVal, color, 0.10, false);
    }}

    // Worker line (on top)
    if (!hiddenLines.has('workers')) {{
      const maxW = Math.max(...TIMELINE.map(p => p[1]), 1);
      drawStepLine(ctx, g, TIMELINE, maxW, '#38bdf8', 0.08, false);
    }}

    // Allocation lines
    if (!hiddenLines.has('alloc-queued') && ALLOC_QUEUED_TL.length >= 2) {{
      const maxV = Math.max(...ALLOC_QUEUED_TL.map(p => p[1]), 1);
      drawStepLine(ctx, g, ALLOC_QUEUED_TL, maxV, '#a3e635', 0, true);
    }}
    // Zoom window highlight
    if (zoomCenterMs !== null) {{
      const half = ZOOM_WINDOW_MS / 2;
      const x1 = toX(zoomCenterMs - half);
      const x2 = toX(zoomCenterMs + half);
      ctx.fillStyle = 'rgba(251,191,36,0.22)';
      ctx.fillRect(x1, PT, x2 - x1, CH);
      ctx.strokeStyle = 'rgba(251,191,36,0.90)';
      ctx.lineWidth = 1.5;
      ctx.setLineDash([3, 3]);
      ctx.beginPath(); ctx.moveTo(x1, PT); ctx.lineTo(x1, PT + CH); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(x2, PT); ctx.lineTo(x2, PT + CH); ctx.stroke();
      ctx.setLineDash([]);
    }}

    // Selected-event vertical line
    if (chartEventIndex !== null) {{
      const ms = new Date(allEvents[chartEventIndex].time).getTime();
      const x  = toX(ms);
      ctx.beginPath(); ctx.moveTo(x, PT); ctx.lineTo(x, PT + CH);
      ctx.strokeStyle = '#fbbf24';
      ctx.lineWidth = 1.5;
      ctx.setLineDash([4, 3]);
      ctx.stroke();
      ctx.setLineDash([]);
    }}

    // X axis time labels
    ctx.fillStyle = '#475569';
    ctx.font = '10px monospace';
    ctx.textAlign = 'left';
    ctx.fillText(new Date(minMs).toISOString().slice(0, 16).replace('T', ' '), PL, H - 2);
    ctx.textAlign = 'right';
    ctx.fillText(new Date(maxMs).toISOString().slice(0, 16).replace('T', ' '), W - PR, H - 2);
  }}

  function updateChartLegend(selectedMs) {{
    const maxW = Math.max(...TIMELINE.map(p => p[1]));
    const wVal = selectedMs !== null ? valueAt(TIMELINE, selectedMs) : null;
    const wLabel = wVal !== null ? wVal + ' / ' + maxW : 'max ' + maxW;

    const hiddenCls = k => hiddenLines.has(k) ? ' hidden-line' : '';
    let html = '<span class="chart-legend-item' + hiddenCls('workers') + '" data-line="workers">'
      + '<span class="chart-legend-line" style="background:#38bdf8"></span>'
      + 'workers: ' + wLabel + '</span>';

    for (const cat of Object.keys({{...TASK_TIMELINES, ...PENDING_TIMELINES}})) {{
      const color = CATEGORY_INFO[cat]?.color || '#888';

      const runTl  = TASK_TIMELINES[cat];
      const pendTl = PENDING_TIMELINES[cat];

      if (runTl) {{
        const maxR = Math.max(...runTl.map(p => p[1]));
        const rVal = selectedMs !== null ? valueAt(runTl, selectedMs) : null;
        const rLabel = rVal !== null ? rVal + '/' + maxR : 'max ' + maxR;
        html += '<span class="chart-legend-item' + hiddenCls(cat + '-run') + '" data-line="' + cat + '-run">'
          + '<span class="chart-legend-line" style="background:' + color + '"></span>'
          + cat + ' run: ' + rLabel + '</span>';
      }}

      if (pendTl) {{
        const maxP = Math.max(...pendTl.map(p => p[1]));
        const pVal = selectedMs !== null ? valueAt(pendTl, selectedMs) : null;
        const pLabel = pVal !== null ? pVal + '/' + maxP : 'max ' + maxP;
        html += '<span class="chart-legend-item' + hiddenCls(cat + '-pend') + '" data-line="' + cat + '-pend">'
          + '<span class="chart-legend-line" style="background:' + color + ';opacity:0.5"></span>'
          + cat + ' pend: ' + pLabel + '</span>';
      }}
    }}
    if (ALLOC_QUEUED_TL.length >= 2) {{
      const maxV = Math.max(...ALLOC_QUEUED_TL.map(p => p[1]));
      const val  = selectedMs !== null ? valueAt(ALLOC_QUEUED_TL, selectedMs) : null;
      const lbl  = val !== null ? val + '/' + maxV : 'max ' + maxV;
      html += '<span class="chart-legend-item' + hiddenCls('alloc-queued') + '" data-line="alloc-queued">'
        + '<span class="chart-legend-line" style="background:#a3e635;opacity:0.7"></span>'
        + 'alloc queued: ' + lbl + '</span>';
    }}
    chartLegendEl.innerHTML = html;
  }}

  function refreshChart(eventIndex, workerId) {{
    chartEventIndex = eventIndex;
    chartWorkerId   = workerId;
    const selectedMs = eventIndex !== null ? new Date(allEvents[eventIndex].time).getTime() : null;
    drawChart();
    drawZoomChart();
    updateChartLegend(selectedMs);
    if (selectedWorkerId !== null) drawWorkerChart();
  }}

  // Precompute event timestamps for fast nearest-event lookup
  const eventMs = allEvents.map(ev => new Date(ev.time).getTime());
  if (eventMs.length > 0) {{
    const totalMin = eventMs[0], totalMax = eventMs[eventMs.length - 1];
    const mid = totalMin + (totalMax - totalMin) / 2;
    const half = ZOOM_WINDOW_MS / 2;
    zoomCenterMs = Math.max(totalMin + half, Math.min(totalMax - half, mid));
  }}

  canvas.addEventListener('click', function(e) {{
    if (TIMELINE.length < 2) return;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const W = canvas.parentElement.offsetWidth;
    const CW = W - PL - PR;
    if (x < PL || x > W - PR) return;

    const minMs = TIMELINE[0][0];
    const maxMs = TIMELINE[TIMELINE.length - 1][0];
    const span  = (maxMs - minMs) || 1;
    const clickMs = minMs + (x - PL) / CW * span;

    // Move zoom center, clamped so the 1h window stays within the full range
    const half = ZOOM_WINDOW_MS / 2;
    zoomCenterMs = Math.max(minMs + half, Math.min(maxMs - half, clickMs));
    drawChart();
    drawZoomChart();
  }});

  zoomCanvas.addEventListener('click', function(e) {{
    if (TIMELINE.length < 2 || zoomCenterMs === null) return;
    const rect = zoomCanvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const W = zoomCanvas.parentElement.offsetWidth;
    const CW = W - ZPL - ZPR;
    if (x < ZPL || x > W - ZPR) return;

    const half = ZOOM_WINDOW_MS / 2;
    const winMin = zoomCenterMs - half;
    const clickMs = winMin + (x - ZPL) / CW * ZOOM_WINDOW_MS;

    // Find nearest event within zoom window; fall back to global nearest
    let best = -1, bestDist = Infinity;
    const winMax = zoomCenterMs + half;
    for (let i = 0; i < eventMs.length; i++) {{
      if (eventMs[i] < winMin || eventMs[i] > winMax) continue;
      const d = Math.abs(eventMs[i] - clickMs);
      if (d < bestDist) {{ bestDist = d; best = i; }}
    }}
    if (best === -1) {{
      let lo = 0, hi = eventMs.length - 1;
      while (lo < hi) {{
        const mid = (lo + hi) >> 1;
        if (eventMs[mid] < clickMs) lo = mid + 1; else hi = mid;
      }}
      best = (lo > 0 && Math.abs(eventMs[lo-1] - clickMs) <= Math.abs(eventMs[lo] - clickMs)) ? lo-1 : lo;
    }}
    const row = tbody.querySelector('.event-row[data-index="' + best + '"]');
    if (row) {{ selectRow(row); row.scrollIntoView({{block: 'nearest'}}); }}
  }});

  workerChartCanvas.addEventListener('click', function(e) {{
    if (selectedWorkerId === null) return;
    const lt = WORKER_LIFETIMES[selectedWorkerId];
    if (!lt) return;
    const rect = workerChartCanvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const W = workerChartCanvas.parentElement.offsetWidth;
    const CW = W - WC_PL - WC_PR;
    if (x < WC_PL || x > W - WC_PR) return;

    const minMs = lt[0];
    const maxMs = lt[1] !== null ? lt[1] : TIMELINE[TIMELINE.length - 1][0];
    const clickMs = minMs + (x - WC_PL) / CW * (maxMs - minMs);

    // Find nearest event within the worker lifetime window
    let best = -1, bestDist = Infinity;
    for (let i = 0; i < eventMs.length; i++) {{
      if (eventMs[i] < minMs || eventMs[i] > maxMs) continue;
      const d = Math.abs(eventMs[i] - clickMs);
      if (d < bestDist) {{ bestDist = d; best = i; }}
    }}
    if (best === -1) {{
      let lo = 0, hi = eventMs.length - 1;
      while (lo < hi) {{
        const mid = (lo + hi) >> 1;
        if (eventMs[mid] < clickMs) lo = mid + 1; else hi = mid;
      }}
      best = (lo > 0 && Math.abs(eventMs[lo-1] - clickMs) <= Math.abs(eventMs[lo] - clickMs)) ? lo-1 : lo;
    }}
    const row = tbody.querySelector('.event-row[data-index="' + best + '"]');
    if (row) {{ selectRow(row); row.scrollIntoView({{block: 'nearest'}}); }}
  }});

  new ResizeObserver(() => {{ setupCanvas(); drawChart(); }}).observe(canvas.parentElement);
  new ResizeObserver(() => {{ setupZoomCanvas(); drawZoomChart(); }}).observe(zoomCanvas.parentElement);
  new ResizeObserver(() => {{ if (selectedWorkerId !== null) {{ setupWorkerCanvas(); drawWorkerChart(); }} }}).observe(workerChartContainer);
  setupCanvas(); drawChart(); updateChartLegend(null);
  setupZoomCanvas(); drawZoomChart();
  updateRelTimes();

  // ── Keyboard navigation ───────────────────────────────────────────────
  document.addEventListener('keydown', function(e) {{
    if (e.key === 'Escape') {{ closeDetail(); return; }}
    if (!selectedRow) return;
    const visibleRows = Array.from(tbody.querySelectorAll('.event-row:not(.hidden)'));
    const idx = visibleRows.indexOf(selectedRow);
    if (e.key === 'ArrowDown' && idx < visibleRows.length - 1) {{
      e.preventDefault();
      selectRow(visibleRows[idx + 1]);
      visibleRows[idx + 1].scrollIntoView({{block: 'nearest'}});
    }} else if (e.key === 'ArrowUp' && idx > 0) {{
      e.preventDefault();
      selectRow(visibleRows[idx - 1]);
      visibleRows[idx - 1].scrollIntoView({{block: 'nearest'}});
    }}
  }});
}})();
</script>
</body>
</html>
"""


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <events.json> [output.html]", file=sys.stderr)
        sys.exit(1)

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"Error: file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else input_path.with_suffix(".html")

    print(f"Loading events from {input_path}...", file=sys.stderr)
    events = load_events(input_path)
    print(f"Loaded {len(events)} events", file=sys.stderr)

    html_content = render_html(events, str(input_path))
    output_path.write_text(html_content, encoding="utf-8")
    print(f"Written to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
