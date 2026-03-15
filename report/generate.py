#!/usr/bin/env python3
"""
generate.py — Build a self-contained HTML dashboard from pico-launcher JSON results.

Usage:
    python3 report/generate.py results/*.json --output dashboard.html
"""

import argparse
import json
import sys
from pathlib import Path

# ── Chart.js CDN (pinned version) ────────────────────────────────────────────
CHARTJS_CDN = "https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"


def load_results(paths: list[str]) -> list[dict]:
    results = []
    for p in paths:
        with open(p) as f:
            data = json.load(f)
            data["_file"] = Path(p).name
            results.append(data)
    return results


def label(r: dict) -> str:
    return f"{r['scenario'].upper()} / {r['test']}"


# ── Throughput chart data ─────────────────────────────────────────────────────

def throughput_datasets(results: list[dict]) -> tuple[list, list, list]:
    """Returns (labels, mbps_values, gbps_values) for throughput results."""
    rows = [r for r in results if r.get("throughput_mbps") is not None]
    labels = [label(r) for r in rows]
    mbps   = [round(r["throughput_mbps"], 2) for r in rows]
    gbps   = [round(r["throughput_gbps"], 4) for r in rows]
    return labels, mbps, gbps


# ── Latency chart data ────────────────────────────────────────────────────────

def latency_datasets(results: list[dict]):
    rows = [r for r in results if r.get("p50_us") is not None]
    labels = [label(r) for r in rows]

    def field(r, key):
        return r.get(key) or 0

    p50  = [field(r, "p50_us")   for r in rows]
    p95  = [field(r, "p95_us")   for r in rows]
    p99  = [field(r, "p99_us")   for r in rows]
    p999 = [field(r, "p999_us")  for r in rows]
    return labels, p50, p95, p99, p999


# ── Latency distribution (sampled RTT trace) ─────────────────────────────────

def distribution_datasets(results: list[dict]):
    rows = [r for r in results if r.get("latency_samples_us")]
    datasets = []
    colors = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6"]
    for i, r in enumerate(rows):
        samples = r["latency_samples_us"]
        # Downsample to at most 500 points for readability
        step = max(1, len(samples) // 500)
        pts = [{"x": j, "y": samples[j]} for j in range(0, len(samples), step)]
        datasets.append({
            "label": label(r),
            "data": pts,
            "borderColor": colors[i % len(colors)],
            "backgroundColor": "transparent",
            "pointRadius": 1,
        })
    return datasets


# ── Summary table ─────────────────────────────────────────────────────────────

def summary_rows(results: list[dict]) -> list[list]:
    rows = []
    for r in results:
        if r.get("throughput_mbps") is not None:
            rows.append([
                label(r),
                f"{r.get('total_bytes', 0) // (1024*1024)} MiB",
                f"{r.get('duration_ms', 0):.1f} ms",
                f"{r.get('throughput_mbps', 0):.2f}",
                f"{r.get('throughput_gbps', 0):.4f}",
                f"{r.get('deserialise_ms', 0):.2f} ms",
                "—", "—", "—",
            ])
        elif r.get("p50_us") is not None:
            rows.append([
                label(r),
                "—", "—", "—", "—", "—",
                f"{r.get('p50_us', 0)} µs",
                f"{r.get('p99_us', 0)} µs",
                f"{r.get('mean_us', 0):.1f} µs",
            ])
    return rows


# ── HTML template ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>pico-launcher benchmark dashboard</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 2rem; }}
  h1   {{ font-size: 1.6rem; margin-bottom: 0.25rem; }}
  .sub {{ color: #94a3b8; font-size: 0.875rem; margin-bottom: 2rem; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }}
  .card {{ background: #1e293b; border-radius: 0.75rem; padding: 1.25rem; }}
  .card h2 {{ font-size: 1rem; color: #94a3b8; margin-bottom: 1rem; }}
  .full {{ grid-column: 1 / -1; }}
  canvas {{ max-height: 320px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
  th, td {{ padding: 0.5rem 0.75rem; text-align: left; border-bottom: 1px solid #334155; }}
  th {{ color: #94a3b8; font-weight: 600; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #0f172a44; }}
  @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<h1>pico-launcher — IPC Benchmark Dashboard</h1>
<p class="sub">Generated at {timestamp} from {n_files} result file(s)</p>

<div class="grid">

  <!-- Throughput bar chart -->
  <div class="card">
    <h2>Throughput (MB/s)</h2>
    <canvas id="chartThroughput"></canvas>
  </div>

  <!-- Latency percentiles bar chart -->
  <div class="card">
    <h2>Latency Percentiles (µs)</h2>
    <canvas id="chartLatency"></canvas>
  </div>

  <!-- RTT distribution line chart -->
  <div class="card full">
    <h2>RTT Sample Distribution (µs per message)</h2>
    <canvas id="chartDist"></canvas>
  </div>

  <!-- Summary table -->
  <div class="card full">
    <h2>Summary</h2>
    <table>
      <thead><tr>
        <th>Test</th>
        <th>Bytes</th>
        <th>Duration</th>
        <th>MB/s</th>
        <th>Gbps</th>
        <th>Deserialise</th>
        <th>p50</th>
        <th>p99</th>
        <th>Mean RTT</th>
      </tr></thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </div>

</div><!-- grid -->

<script src="{chartjs_cdn}"></script>
<script>
const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#ec4899'];

// Throughput
(function() {{
  const labels = {throughput_labels};
  const mbps   = {throughput_mbps};
  if (!labels.length) return;
  new Chart(document.getElementById('chartThroughput'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [{{ label: 'MB/s', data: mbps,
        backgroundColor: labels.map((_,i) => COLORS[i % COLORS.length]) }}]
    }},
    options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
      scales: {{ y: {{ beginAtZero: true, ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#334155' }} }},
                 x: {{ ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#334155' }} }} }} }}
  }});
}})();

// Latency percentiles
(function() {{
  const labels = {latency_labels};
  const p50    = {latency_p50};
  const p95    = {latency_p95};
  const p99    = {latency_p99};
  const p999   = {latency_p999};
  if (!labels.length) return;
  new Chart(document.getElementById('chartLatency'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{ label: 'p50',   data: p50,  backgroundColor: '#3b82f6' }},
        {{ label: 'p95',   data: p95,  backgroundColor: '#f59e0b' }},
        {{ label: 'p99',   data: p99,  backgroundColor: '#ef4444' }},
        {{ label: 'p99.9', data: p999, backgroundColor: '#8b5cf6' }},
      ]
    }},
    options: {{ responsive: true,
      scales: {{ y: {{ beginAtZero: true, ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#334155' }} }},
                 x: {{ ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#334155' }} }} }} }}
  }});
}})();

// RTT distribution
(function() {{
  const datasets = {dist_datasets};
  if (!datasets.length) return;
  new Chart(document.getElementById('chartDist'), {{
    type: 'scatter',
    data: {{ datasets }},
    options: {{ responsive: true,
      parsing: {{ xAxisKey: 'x', yAxisKey: 'y' }},
      scales: {{
        x: {{ title: {{ display: true, text: 'Message #', color:'#94a3b8' }}, ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#334155' }} }},
        y: {{ title: {{ display: true, text: 'RTT µs',    color:'#94a3b8' }}, ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#334155' }}, beginAtZero: true }}
      }}
    }}
  }});
}})();
</script>
</body>
</html>
"""


def build_table_rows(rows: list[list]) -> str:
    html = ""
    for row in rows:
        cols = "".join(f"<td>{c}</td>" for c in row)
        html += f"<tr>{cols}</tr>\n        "
    return html or "<tr><td colspan='9'>No data</td></tr>"


def generate(paths: list[str], output: str) -> None:
    results = load_results(paths)
    import datetime

    t_labels, t_mbps, _ = throughput_datasets(results)
    l_labels, l_p50, l_p95, l_p99, l_p999 = latency_datasets(results)
    dist = distribution_datasets(results)
    table = summary_rows(results)

    html = HTML_TEMPLATE.format(
        timestamp=datetime.datetime.now().isoformat(timespec="seconds"),
        n_files=len(paths),
        chartjs_cdn=CHARTJS_CDN,
        throughput_labels=json.dumps(t_labels),
        throughput_mbps=json.dumps(t_mbps),
        latency_labels=json.dumps(l_labels),
        latency_p50=json.dumps(l_p50),
        latency_p95=json.dumps(l_p95),
        latency_p99=json.dumps(l_p99),
        latency_p999=json.dumps(l_p999),
        dist_datasets=json.dumps(dist),
        table_rows=build_table_rows(table),
    )

    Path(output).write_text(html, encoding="utf-8")
    print(f"Dashboard written to {output}  ({len(html):,} bytes)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate HTML benchmark dashboard")
    parser.add_argument("files", nargs="+", help="JSON result files from the receiver")
    parser.add_argument("--output", default="dashboard.html", help="Output HTML path")
    args = parser.parse_args()

    missing = [f for f in args.files if not Path(f).exists()]
    if missing:
        print(f"ERROR: file(s) not found: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    generate(args.files, args.output)


if __name__ == "__main__":
    main()
