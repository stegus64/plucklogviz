#!/usr/bin/env python3
"""Render a Gantt-style visualization from pluck-like logs.

The parser only relies on:
- line timestamp (HH:MM:SS)
- stream=... and chunk=... tags
"""

from __future__ import annotations

import argparse
import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

TIME_RE = re.compile(r"^(\d{2}:\d{2}:\d{2})\b")
STREAM_RE = re.compile(r"\bstream=([^\]\s]+)")
CHUNK_RE = re.compile(r"\bchunk=([^\]\s]+)")
ROWS_RE = re.compile(r"\brows=(\d+)\b")


@dataclass
class ChunkWindow:
    stream: str
    chunk: str
    start_s: int
    end_s: int
    line_count: int = 0
    rows_processed: int = 0

    @property
    def duration_s(self) -> int:
        return max(0, self.end_s - self.start_s)


@dataclass
class ParsedData:
    chunks: List[ChunkWindow]
    timeline_start: int
    timeline_end: int
    completed_streams: Set[str]
    stream_errors: Dict[str, List[str]]
    failed_chunks: Set[Tuple[str, str]]


def hms_to_seconds(hms: str) -> int:
    h, m, s = map(int, hms.split(":"))
    return h * 3600 + m * 60 + s


def chunk_sort_value(chunk: str) -> Tuple[int, str]:
    if chunk.isdigit():
        return (0, f"{int(chunk):09d}")
    return (1, chunk)


def parse_log(path: Path) -> ParsedData:
    open_windows: Dict[Tuple[str, str], ChunkWindow] = {}
    completed_streams: Set[str] = set()
    stream_errors: Dict[str, List[str]] = {}
    failed_chunks: Set[Tuple[str, str]] = set()
    last_raw_ts: Optional[int] = None
    day_offset = 0

    timeline_start: Optional[int] = None
    timeline_end: Optional[int] = None

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            tmatch = TIME_RE.match(line)
            if not tmatch:
                continue

            raw_ts = hms_to_seconds(tmatch.group(1))
            if last_raw_ts is not None and raw_ts < last_raw_ts:
                day_offset += 86400
            last_raw_ts = raw_ts
            ts = raw_ts + day_offset

            sm = STREAM_RE.search(line)
            cm = CHUNK_RE.search(line)

            if sm:
                stream = sm.group(1)
                if "fail:" in line:
                    stream_errors.setdefault(stream, []).append(line.strip())
                    if cm:
                        failed_chunks.add((stream, cm.group(1)))
                if "complete ===" in line:
                    completed_streams.add(stream)

            if sm and cm:
                stream = sm.group(1)
                chunk = cm.group(1)
                key = (stream, chunk)
                rows_match = ROWS_RE.search(line)
                rows_value = int(rows_match.group(1)) if rows_match else None

                if key not in open_windows:
                    open_windows[key] = ChunkWindow(
                        stream=stream,
                        chunk=chunk,
                        start_s=ts,
                        end_s=ts,
                        line_count=1,
                        rows_processed=rows_value or 0,
                    )
                else:
                    cw = open_windows[key]
                    if ts < cw.start_s:
                        cw.start_s = ts
                    if ts > cw.end_s:
                        cw.end_s = ts
                    cw.line_count += 1
                    if rows_value is not None:
                        cw.rows_processed = max(cw.rows_processed, rows_value)

            if timeline_start is None or ts < timeline_start:
                timeline_start = ts
            if timeline_end is None or ts > timeline_end:
                timeline_end = ts

    chunks = sorted(
        open_windows.values(),
        key=lambda x: (x.stream, chunk_sort_value(x.chunk), x.start_s),
    )

    if not chunks:
        raise ValueError("No lines with both stream=... and chunk=... were found.")

    assert timeline_start is not None and timeline_end is not None
    return ParsedData(
        chunks=chunks,
        timeline_start=timeline_start,
        timeline_end=timeline_end,
        completed_streams=completed_streams,
        stream_errors=stream_errors,
        failed_chunks=failed_chunks,
    )


def status_color(status: str) -> str:
    if status == "error":
        return "#dc2626"
    if status == "complete":
        return "#16a34a"
    return "#6b7280"


def seconds_label(s: int) -> str:
    h = (s // 3600) % 24
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def duration_label(total_seconds: int) -> str:
    total_seconds = max(0, total_seconds)
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    sec = total_seconds % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def absolute_time_label(ts_abs: int) -> str:
    day = ts_abs // 86400
    base = seconds_label(ts_abs)
    if day <= 0:
        return base
    return f"D+{day} {base}"


def format_int(n: int) -> str:
    return f"{n:,}"


def render_html(data: ParsedData, title: str) -> str:
    chunks = data.chunks

    left_pad = 260
    right_pad = 32
    top_pad = 52
    row_h = 24
    bar_h = 14
    chart_w = 1400
    stream_windows: Dict[str, Tuple[int, int, int]] = {}
    for c in chunks:
        if c.stream not in stream_windows:
            stream_windows[c.stream] = (c.start_s, c.end_s, 1)
        else:
            s0, s1, n = stream_windows[c.stream]
            stream_windows[c.stream] = (min(s0, c.start_s), max(s1, c.end_s), n + 1)

    stream_order = sorted(stream_windows.keys(), key=lambda s: (stream_windows[s][0], s))
    chunks_by_stream: Dict[str, List[ChunkWindow]] = {s: [] for s in stream_order}
    for c in chunks:
        chunks_by_stream[c.stream].append(c)
    for s in chunks_by_stream:
        chunks_by_stream[s].sort(key=lambda c: (chunk_sort_value(c.chunk), c.start_s))
    stream_total_rows: Dict[str, int] = {
        s: sum(c.rows_processed for c in chunks_by_stream[s]) for s in stream_order
    }
    global_total_rows = sum(c.rows_processed for c in chunks)

    # Initial collapsed height: summary rows only. JS adjusts height when expanded.
    height = top_pad + row_h * len(stream_order) + 64

    total_span = max(1, data.timeline_end - data.timeline_start)

    def x_at(ts: int) -> float:
        return left_pad + ((ts - data.timeline_start) / total_span) * chart_w

    tick_step_s = 3600
    first_tick = (data.timeline_start // tick_step_s) * tick_step_s
    if first_tick < data.timeline_start:
        first_tick += tick_step_s
    tick_lines: List[str] = []
    t = first_tick
    while t <= data.timeline_end:
        x = x_at(t)
        tick_lines.append(
            f'<line x1="{x:.2f}" y1="{top_pad - 16}" x2="{x:.2f}" y2="{height - 28}" stroke="#e5e7eb" stroke-width="1" />'
            f'<text x="{x:.2f}" y="{top_pad - 22}" class="tick" text-anchor="middle">{absolute_time_label(t)}</text>'
        )
        t += tick_step_s

    row_svg: List[str] = []
    row_order: List[str] = []
    for stream in stream_order:
        s0, s1, n_chunks = stream_windows[stream]
        stream_status = "running"
        if stream in data.stream_errors:
            stream_status = "error"
        elif stream in data.completed_streams:
            stream_status = "complete"
        fill = status_color(stream_status)
        start_x = x_at(s0)
        end_x = x_at(s1)
        width = max(2.0, end_x - start_x)
        stream_label = f"{stream} ({n_chunks} chunks)"
        stream_error_text = "\n\n".join(data.stream_errors.get(stream, []))
        stream_title = (
            f"{stream} | status={stream_status} | start={absolute_time_label(s0)} | "
            f"end={absolute_time_label(s1)} | duration={duration_label(max(0, s1 - s0))} | "
            f"chunks={n_chunks} | rows={format_int(stream_total_rows[stream])}"
        )
        if stream_error_text:
            stream_title += f"\n\nException:\n{stream_error_text}"
        row_svg.append(
            f'<g class="row summary-row" data-row-type="summary" data-stream="{html.escape(stream)}" tabindex="0" role="button" '
            f'onclick="toggleStream(\'{html.escape(stream)}\')" onkeydown="rowKey(event, \'{html.escape(stream)}\')">'
            f'<text x="{left_pad - 10}" y="{bar_h - 1}" class="label summary-label" text-anchor="end">{html.escape(stream_label)}</text>'
            f'<rect x="{start_x:.2f}" y="0" width="{width:.2f}" height="{bar_h}" rx="3" ry="3" '
            f'fill="{fill}" opacity="0.86">'
            f'<title>{html.escape(stream_title)}</title>'
            f'</rect>'
            f"</g>"
        )
        row_order.append(stream)

        for chunk in chunks_by_stream[stream]:
            chunk_status = stream_status
            if (stream, chunk.chunk) in data.failed_chunks:
                chunk_status = "error"
            chunk_fill = status_color(chunk_status)
            c_start_x = x_at(chunk.start_s)
            c_end_x = x_at(chunk.end_s)
            c_width = max(2.0, c_end_x - c_start_x)
            label = f"{chunk.stream} / chunk={chunk.chunk}"
            row_svg.append(
                f'<g class="row chunk-row" data-row-type="chunk" data-stream="{html.escape(stream)}" style="display:none">'
                f'<text x="{left_pad - 10}" y="{bar_h - 1}" class="label" text-anchor="end">{html.escape(label)}</text>'
                f'<rect x="{c_start_x:.2f}" y="0" width="{c_width:.2f}" height="{bar_h}" rx="3" ry="3" '
                f'fill="{chunk_fill}" opacity="0.58">'
                f'<title>{html.escape(label)} | status={chunk_status} | start={absolute_time_label(chunk.start_s)} | end={absolute_time_label(chunk.end_s)} | duration={duration_label(chunk.duration_s)} | rows={format_int(chunk.rows_processed)}</title>'
                f"</rect>"
                f"</g>"
            )

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 16px; color: #0f172a; }}
    h1 {{ font-size: 1.1rem; margin: 0 0 10px; }}
    .meta {{ margin: 0 0 14px; color: #475569; font-size: .92rem; }}
    .chart-wrap {{ border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; overflow-x: auto; background: #fff; }}
    svg {{ min-width: {left_pad + right_pad + chart_w}px; }}
    .tick {{ font-size: 10px; fill: #64748b; }}
    .label {{ font-size: 11px; fill: #1f2937; }}
    .summary-row {{ cursor: pointer; }}
    .summary-row:focus {{ outline: none; }}
    .summary-label {{ font-weight: 600; }}
    .summary-row.active .summary-label {{ fill: #0f172a; text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p class=\"meta\">Collapsed: 1 bar per stream. Click a stream bar to expand its chunks. Total chunks: {len(chunks)} | Total rows: {global_total_rows} | Timeline: {absolute_time_label(data.timeline_start)} to {absolute_time_label(data.timeline_end)} ({duration_label(total_span)})</p>
  <div class=\"chart-wrap\">
    <svg id=\"gantt\" width=\"{left_pad + right_pad + chart_w}\" height=\"{height}\" role=\"img\" aria-label=\"Chunk Gantt chart\">
      <rect x=\"0\" y=\"0\" width=\"100%\" height=\"100%\" fill=\"white\"/>
      {''.join(tick_lines).replace('<line ', '<line class="grid-tick" ')}
      {''.join(row_svg)}
    </svg>
  </div>
  <script>
    const rowHeight = {row_h};
    const topPad = {top_pad};
    const streamOrder = {json.dumps(row_order)};
    const svg = document.getElementById("gantt");
    let expandedStream = null;

    function rowKey(evt, stream) {{
      if (evt.key === "Enter" || evt.key === " ") {{
        evt.preventDefault();
        toggleStream(stream);
      }}
    }}

    function toggleStream(stream) {{
      expandedStream = expandedStream === stream ? null : stream;
      layoutRows();
    }}

    function layoutRows() {{
      let y = topPad;
      document.querySelectorAll(".row").forEach((row) => {{
        const type = row.dataset.rowType;
        const stream = row.dataset.stream;
        const visible = type === "summary" || (expandedStream !== null && expandedStream === stream);
        row.style.display = visible ? "" : "none";
        row.classList.toggle("active", type === "summary" && expandedStream === stream);
        if (visible) {{
          row.setAttribute("transform", `translate(0,${{y}})`);
          y += rowHeight;
        }}
      }});

      const h = y + 64;
      svg.setAttribute("height", String(h));
      document.querySelectorAll(".grid-tick").forEach((line) => {{
        line.setAttribute("y2", String(h - 28));
      }});
    }}

    layoutRows();
  </script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a Gantt HTML from a pluck-style log.")
    parser.add_argument("input", help="Path to log file (e.g. pluck.log)")
    parser.add_argument("-o", "--output", default="gantt.html", help="Output HTML path")
    parser.add_argument("--title", default="Pluck Log Chunk Timeline", help="Chart title")
    args = parser.parse_args()

    data = parse_log(Path(args.input))
    output_html = render_html(data, args.title)

    out_path = Path(args.output)
    out_path.write_text(output_html, encoding="utf-8")
    print(f"Wrote {out_path} with {len(data.chunks)} chunk bars.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
