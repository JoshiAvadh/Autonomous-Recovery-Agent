
"""
generate_charts_20.py
Runs a fresh 20-node simulation using the API and generates the same
presentation-ready charts as PNG files in a charts_20/ folder.

Default behavior:
    - Uses API by default
    - Runs ONLY a 20-node simulation

Usage:
    python3 generate_charts_20.py
    python3 generate_charts_20.py --logs   # optional: parse existing 20-node logs instead
"""

import os
import sys
import re
import glob
import argparse
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))

CHART_DIR = os.path.join(os.path.dirname(__file__), "charts_20")
os.makedirs(CHART_DIR, exist_ok=True)


DARK_BG = "#0d1520"
SURFACE = "#1a2a3a"
TEXT = "#c8d8e8"
ACCENT = "#00d4ff"
GREEN = "#00ff88"
RED = "#ff4466"
ORANGE = "#ffaa00"
PURPLE = "#aa88ff"
PINK = "#ff88ff"

plt.rcParams.update({
    "figure.facecolor": DARK_BG,
    "axes.facecolor": DARK_BG,
    "axes.edgecolor": SURFACE,
    "axes.labelcolor": TEXT,
    "text.color": TEXT,
    "xtick.color": TEXT,
    "ytick.color": TEXT,
    "legend.facecolor": SURFACE,
    "legend.edgecolor": SURFACE,
    "legend.labelcolor": TEXT,
    "font.family": "monospace",
    "font.size": 11,
    "figure.dpi": 150,
})


def save_chart(fig, name):
    path = os.path.join(CHART_DIR, f"{name}.png")
    fig.savefig(path, bbox_inches="tight", facecolor=DARK_BG, pad_inches=0.3)
    plt.close(fig)
    print(f"  ✓ {path}")




def _extract_float(text, pattern, default=0.0):
    m = re.search(pattern, text)
    return float(m.group(1)) if m else default


def _extract_int(text, pattern, default=0):
    m = re.search(pattern, text)
    return int(m.group(1)) if m else default


def parse_log(filepath):
    """Parse a simulation log file and extract all metrics."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    m = re.search(r"Building cluster with (\d+) nodes", content)
    if not m:
        return None
    n_nodes = int(m.group(1))

    commit_rate = _extract_float(content, r"Commit rate:\s+([\d.]+)%")
    delivery_rate = _extract_float(content, r"Message delivery:\s+([\d.]+)%")
    avg_mttr = _extract_float(content, r"Avg MTTR:\s+([\d.]+)")
    timeout_rate = _extract_float(content, r"Timeout rate:\s+([\d.]+)%")
    leader_elections = _extract_int(content, r"Leader elections:\s+(\d+)")
    split_brain = _extract_int(content, r"Split-brain events:\s+(\d+)")
    quorum_unavailable = _extract_float(content, r"Quorum unavailable:\s+([\d.]+)")
    total_llm = _extract_int(content, r"Total LLM decisions:\s+(\d+)")
    novel_overrides = _extract_int(content, r"LLM novel overrides:\s+(\d+)")

    m = re.search(r"Failures resolved:\s+(\d+)/(\d+)", content)
    if m:
        resolved = int(m.group(1))
        total_failures = int(m.group(2))
    else:
        resolved = total_failures = 0

    m = re.search(r"Commit rate:\s+[\d.]+%\s+\((\d+)/(\d+)\)", content)
    commits_ok = int(m.group(1)) if m else 0
    commits_total = int(m.group(2)) if m else 0

    scores = []
    for sm in re.finditer(
        r"\[t=([\d.]+)\]\s+\[SCORE\]\s+(\S+):\s+total=(\d+)\s+"
        r"\(speed=(\d+),\s*stability=(\d+),\s*cascade=(\d+),\s*delivery=(\d+)\)\s+source=(\w+)",
        content
    ):
        scores.append({
            "time": float(sm.group(1)),
            "node": sm.group(2),
            "total_score": int(sm.group(3)),
            "speed_score": int(sm.group(4)),
            "stability_score": int(sm.group(5)),
            "cascade_score": int(sm.group(6)),
            "delivery_score": int(sm.group(7)),
            "source": sm.group(8),
            "action": "unknown",
        })

    decision_actions = []
    for dm in re.finditer(
        r"\[t=([\d.]+)\].*?Action:\s+(\w+)\s+on\s+(\S+)",
        content
    ):
        decision_actions.append({
            "time": float(dm.group(1)),
            "action": dm.group(2),
            "target": dm.group(3),
        })

    for s in scores:
        best = None
        for d in decision_actions:
            if d["time"] <= s["time"]:
                if best is None or d["time"] > best["time"]:
                    best = d
        if best:
            s["action"] = best["action"]

    source_summary = {}
    for s in scores:
        src = s["source"]
        if src not in source_summary:
            source_summary[src] = {"count": 0, "total": 0, "resolved": 0}
        source_summary[src]["count"] += 1
        source_summary[src]["total"] += s["total_score"]

    predictive_count = len(re.findall(r"\[PREDICTIVE\]", content))
    proactive_count = len(re.findall(r"proactive_optimization", content))

    return {
        "n_nodes": n_nodes,
        "filepath": filepath,
        "commit_rate": commit_rate,
        "delivery_rate": delivery_rate,
        "avg_mttr": avg_mttr,
        "timeout_rate": timeout_rate,
        "leader_elections": leader_elections,
        "split_brain": split_brain,
        "quorum_unavailable": quorum_unavailable,
        "total_failures": total_failures,
        "resolved": resolved,
        "total_llm": total_llm,
        "novel_overrides": novel_overrides,
        "scores": scores,
        "source_summary": source_summary,
        "predictive_count": predictive_count,
        "proactive_count": proactive_count,
        "commits_ok": commits_ok,
        "commits_total": commits_total,
    }


def find_best_logs(base_dir, target_sizes=None):
    """Find the best simulation log for each cluster size."""
    log_files = sorted(glob.glob(os.path.join(base_dir, "simulation_*.txt")))
    by_size = {}

    for lf in log_files:
        parsed = parse_log(lf)
        if parsed is None:
            continue
        n = parsed["n_nodes"]
        if target_sizes and n not in target_sizes:
            continue
        by_size.setdefault(n, []).append(parsed)

    best = {}
    for n, candidates in by_size.items():
        min_writes = max(10, n)
        good = [c for c in candidates if c["commits_total"] >= min_writes]
        if not good:
            good = [c for c in candidates if c["commits_total"] > 0]
        if not good:
            good = candidates

        good.sort(key=lambda c: (c["commit_rate"], c["commits_total"]), reverse=True)
        best[n] = good[0]

    return best


def run_fresh_simulations(sizes):
    """Run fresh simulations — uses API."""
    from main import run_simulation

    results = {}
    for n in sizes:
        print(f"\n▶ Running {n}-node simulation using API...")
        try:
            res = run_simulation(n)
            net_stats = res["network"].get_stats()
            scores = res["scorer"].get_scores()
            injector = res["injector"]

            resolved_list = [
                f for f in injector.history
                if getattr(f, "resolved", False) and getattr(f, "resolved_at", None) is not None
            ]
            avg_mttr = (
                sum(f.resolved_at - f.time for f in resolved_list) / len(resolved_list)
                if resolved_list else 0
            )

            parsed_scores = []
            for s in scores:
                parsed_scores.append({
                    "time": float(s.get("time", 0)),
                    "node": s.get("target", "unknown"),
                    "total_score": int(s.get("total_score", 0)),
                    "speed_score": int(s.get("speed_score", 0)),
                    "stability_score": int(s.get("stability_score", 0)),
                    "cascade_score": int(s.get("cascade_score", 0)),
                    "delivery_score": int(s.get("delivery_score", 0)),
                    "source": s.get("source", "unknown"),
                    "action": s.get("action", "unknown"),
                })

            commits_ok = int(net_stats.get("writes_committed", 0))
            commits_total = int(net_stats.get("writes_attempted", 0))
            total_llm = int(res.get("total_llm_decisions", 0)) if isinstance(res, dict) else 0
            novel_overrides = int(res.get("llm_novel_overrides", 0)) if isinstance(res, dict) else 0

            results[n] = {
                "n_nodes": n,
                "filepath": None,
                "commit_rate": float(str(net_stats["commit_rate"]).replace("%", "")),
                "delivery_rate": float(str(net_stats["success_rate"]).replace("%", "")),
                "avg_mttr": avg_mttr,
                "timeout_rate": float(str(net_stats["timeout_rate"]).replace("%", "")),
                "leader_elections": int(net_stats.get("leader_elections", 0)),
                "split_brain": int(net_stats.get("split_brain_events", 0)),
                "quorum_unavailable": float(net_stats.get("quorum_unavailable_ticks", 0)),
                "total_failures": len(injector.history),
                "resolved": len(resolved_list),
                "total_llm": total_llm,
                "novel_overrides": novel_overrides,
                "scores": parsed_scores,
                "source_summary": {},
                "predictive_count": 0,
                "proactive_count": 0,
                "commits_ok": commits_ok,
                "commits_total": commits_total,
            }

            print(
                f"  ✓ {n} nodes: {len(resolved_list)}/{len(injector.history)} resolved, "
                f"commit={net_stats['commit_rate']}, delivery={net_stats['success_rate']}"
            )
        except Exception as e:
            print(f"  ✗ {n} nodes failed: {e}")
            results[n] = None

    return results


parser = argparse.ArgumentParser()
parser.add_argument(
    "--logs",
    action="store_true",
    help="Parse existing logs instead of running fresh API simulations"
)
args = parser.parse_args()

sizes = [20]

print("=" * 50)
print("  GENERATING 20-NODE PRESENTATION CHARTS")
print("=" * 50)

if args.logs:
    print("\n  Mode: PARSING EXISTING LOGS (zero API tokens)")
    base_dir = os.path.dirname(__file__)
    all_logs = find_best_logs(base_dir, target_sizes=sizes)

    valid = {}
    for n in sizes:
        if n in all_logs:
            valid[n] = all_logs[n]
            print(
                f"  ✓ {n} nodes: {os.path.basename(all_logs[n]['filepath'])} "
                f"— commit={all_logs[n]['commit_rate']:.1f}%, delivery={all_logs[n]['delivery_rate']:.1f}%"
            )

    if not valid:
        print("  ✗ No matching 20-node simulation logs found! Run: python3 main.py 20")
        sys.exit(1)
else:
    print("\n  Mode: FRESH 20-NODE SIMULATION (uses API tokens)")
    valid = run_fresh_simulations(sizes)
    valid = {k: v for k, v in valid.items() if v}

    if not valid:
        print("  ✗ No successful simulation run produced results.")
        sys.exit(1)

valid = dict(sorted(valid.items()))

print(f"\n{'=' * 50}")
print("  GENERATING CHARTS...")
print(f"{'=' * 50}\n")

# Use one label only
labels = [f"{k} Nodes" for k in valid]
x = np.arange(len(valid))
width = 0.3


fig, ax = plt.subplots(figsize=(8, 5))
commit_rates = [v["commit_rate"] for v in valid.values()]
delivery_rates = [v["delivery_rate"] for v in valid.values()]

bars1 = ax.bar(x - width / 2, commit_rates, width, label="Commit Rate", color=GREEN, alpha=0.9)
bars2 = ax.bar(x + width / 2, delivery_rates, width, label="Delivery Rate", color=ACCENT, alpha=0.9)

for bar in bars1:
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
            f"{bar.get_height():.1f}%", ha="center", va="bottom",
            fontsize=10, color=GREEN, fontweight="bold")
for bar in bars2:
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
            f"{bar.get_height():.1f}%", ha="center", va="bottom",
            fontsize=10, color=ACCENT, fontweight="bold")

ax.set_ylabel("Rate (%)")
ax.set_title("20-Node Simulation: Commit Rate & Message Delivery", fontsize=14, fontweight="bold", color=ACCENT)
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_ylim(0, 110)
ax.legend()
ax.grid(axis="y", alpha=0.15, color=TEXT)
save_chart(fig, "1_commit_delivery_rates_20")


fig, ax = plt.subplots(figsize=(8, 5))
total_f = [v["total_failures"] for v in valid.values()]
resolved_f = [v["resolved"] for v in valid.values()]
active_f = [t - r for t, r in zip(total_f, resolved_f)]

bars1 = ax.bar(x - width / 2, resolved_f, width, label="Resolved", color=GREEN, alpha=0.9)
bars2 = ax.bar(x + width / 2, active_f, width, label="Active (unresolved)", color=RED, alpha=0.9)

for i, bar in enumerate(bars1):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
            str(resolved_f[i]), ha="center", va="bottom",
            fontsize=11, color=GREEN, fontweight="bold")
for i, bar in enumerate(bars2):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
            str(active_f[i]), ha="center", va="bottom",
            fontsize=11, color=RED, fontweight="bold")

ax.set_ylabel("Number of Failures")
ax.set_title("20-Node Simulation: Failure Resolution", fontsize=14, fontweight="bold", color=ACCENT)
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.grid(axis="y", alpha=0.15, color=TEXT)
save_chart(fig, "2_failure_resolution_20")


fig, ax = plt.subplots(figsize=(8, 5))
mttrs = [v["avg_mttr"] for v in valid.values()]
bars = ax.bar(labels, mttrs, color=ORANGE, alpha=0.9, width=0.5)
for bar, val in zip(bars, mttrs):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
            f"{val:.1f}", ha="center", va="bottom",
            fontsize=12, color=ORANGE, fontweight="bold")
ax.set_ylabel("Avg MTTR (ticks)")
ax.set_title("20-Node Simulation: Mean Time to Recovery", fontsize=14, fontweight="bold", color=ACCENT)
ax.set_ylim(0, max(mttrs) * 1.5 if mttrs else 10)
ax.grid(axis="y", alpha=0.15, color=TEXT)
save_chart(fig, "3_mttr_20")


fig, ax = plt.subplots(figsize=(10, 5))
all_llm_scores = []
all_rule_scores = []
all_proactive_scores = []

for v in valid.values():
    for s in v["scores"]:
        if s["source"] in ("llm", "llm_async"):
            all_llm_scores.append(s["total_score"])
        elif s["source"] == "rule":
            all_rule_scores.append(s["total_score"])
        elif s["source"] == "llm_proactive":
            all_proactive_scores.append(s["total_score"])

categories = []
avgs = []
colors = []
counts = []

if all_llm_scores:
    categories.append("LLM\nReactive")
    avgs.append(np.mean(all_llm_scores))
    colors.append(GREEN)
    counts.append(len(all_llm_scores))
if all_rule_scores:
    categories.append("Rule\nEngine")
    avgs.append(np.mean(all_rule_scores))
    colors.append(ORANGE)
    counts.append(len(all_rule_scores))
if all_proactive_scores:
    categories.append("LLM\nProactive")
    avgs.append(np.mean(all_proactive_scores))
    colors.append(PINK)
    counts.append(len(all_proactive_scores))

if categories:
    bars = ax.bar(categories, avgs, color=colors, alpha=0.9, width=0.5)
    for bar, val, cnt in zip(bars, avgs, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{val:.1f}", ha="center", va="bottom",
                fontsize=13, fontweight="bold", color=bar.get_facecolor())
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2,
                f"n={cnt}", ha="center", va="center",
                fontsize=9, color=DARK_BG, fontweight="bold")

ax.set_ylabel("Avg Decision Score (0-100)")
ax.set_title("20-Node Simulation: LLM vs Rule Engine Decision Quality", fontsize=14, fontweight="bold", color=ACCENT)
ax.set_ylim(0, 100)
ax.axhline(y=70, color=TEXT, linestyle="--", alpha=0.3, label="Good threshold")
ax.grid(axis="y", alpha=0.15, color=TEXT)
save_chart(fig, "4_llm_vs_rules_score_20")

fig, ax = plt.subplots(figsize=(10, 5))
components = ["Speed\n(MTTR)", "Stability\n(Health Δ)", "Cascade\n(Prevention)", "Delivery\n(Msg Rate)"]
weights = [0.35, 0.25, 0.25, 0.15]

llm_comps = [0, 0, 0, 0]
rule_comps = [0, 0, 0, 0]
proactive_comps = [0, 0, 0, 0]
llm_n = rule_n = proactive_n = 0

for v in valid.values():
    for s in v["scores"]:
        vals = [s["speed_score"], s["stability_score"], s["cascade_score"], s["delivery_score"]]
        if s["source"] in ("llm", "llm_async"):
            for i in range(4):
                llm_comps[i] += vals[i]
            llm_n += 1
        elif s["source"] == "rule":
            for i in range(4):
                rule_comps[i] += vals[i]
            rule_n += 1
        elif s["source"] == "llm_proactive":
            for i in range(4):
                proactive_comps[i] += vals[i]
            proactive_n += 1

x_comp = np.arange(len(components))
width_comp = 0.25
datasets = []
if llm_n:
    datasets.append(("LLM Reactive", [c / llm_n for c in llm_comps], GREEN))
if rule_n:
    datasets.append(("Rule Engine", [c / rule_n for c in rule_comps], ORANGE))
if proactive_n:
    datasets.append(("LLM Proactive", [c / proactive_n for c in proactive_comps], PINK))

for i, (label, vals, color) in enumerate(datasets):
    offset = (i - len(datasets) / 2 + 0.5) * width_comp
    bars = ax.bar(x_comp + offset, vals, width_comp, label=label, color=color, alpha=0.85)
    for bar in bars:
        if bar.get_height() > 5:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                    f"{bar.get_height():.0f}", ha="center", va="bottom",
                    fontsize=9, color=color)

for i, w in enumerate(weights):
    ax.text(i, -8, f"weight: {w}", ha="center", fontsize=8, color=TEXT, alpha=0.6)

ax.set_ylabel("Score (0-100)")
ax.set_title("20-Node Simulation: Decision Score Breakdown", fontsize=14, fontweight="bold", color=ACCENT)
ax.set_xticks(x_comp)
ax.set_xticklabels(components)
ax.set_ylim(-12, 110)
ax.legend(loc="upper right")
ax.grid(axis="y", alpha=0.15, color=TEXT)
save_chart(fig, "5_score_breakdown_20")

fig, ax = plt.subplots(figsize=(8, 8))
action_counts = {}
for v in valid.values():
    for s in v["scores"]:
        a = s["action"]
        action_counts[a] = action_counts.get(a, 0) + 1

sorted_actions = sorted(action_counts.items(), key=lambda x: x[1], reverse=True)
action_labels = [a.replace("_", " ").title() for a, _ in sorted_actions]
action_vals = [c for _, c in sorted_actions]
pie_colors = [GREEN, ACCENT, ORANGE, PURPLE, PINK, RED, TEXT, "#446688"][:len(action_labels)]

if action_vals:
    wedges, texts, autotexts = ax.pie(
        action_vals,
        labels=action_labels,
        colors=pie_colors,
        autopct="%1.0f%%",
        startangle=90,
        pctdistance=0.8,
        textprops={"fontsize": 10, "color": TEXT},
    )
    for at in autotexts:
        at.set_fontsize(9)
        at.set_color(DARK_BG)
        at.set_fontweight("bold")

ax.set_title("20-Node Simulation: Distribution of Recovery Actions", fontsize=14, fontweight="bold", color=ACCENT)
save_chart(fig, "6_action_distribution_20")

largest_scores = valid[20]["scores"]

if largest_scores:
    fig, ax = plt.subplots(figsize=(12, 5))

    llm_times = [s["time"] for s in largest_scores if s["source"] in ("llm", "llm_async")]
    llm_vals = [s["total_score"] for s in largest_scores if s["source"] in ("llm", "llm_async")]
    rule_times = [s["time"] for s in largest_scores if s["source"] == "rule"]
    rule_vals = [s["total_score"] for s in largest_scores if s["source"] == "rule"]
    pro_times = [s["time"] for s in largest_scores if s["source"] == "llm_proactive"]
    pro_vals = [s["total_score"] for s in largest_scores if s["source"] == "llm_proactive"]

    if llm_times:
        ax.scatter(llm_times, llm_vals, c=GREEN, s=60, label="LLM Reactive", zorder=3, alpha=0.8)
        ax.plot(llm_times, llm_vals, color=GREEN, alpha=0.3, linewidth=1)
    if rule_times:
        ax.scatter(rule_times, rule_vals, c=ORANGE, s=60, label="Rule Engine", zorder=3, marker="s", alpha=0.8)
        ax.plot(rule_times, rule_vals, color=ORANGE, alpha=0.3, linewidth=1)
    if pro_times:
        ax.scatter(pro_times, pro_vals, c=PINK, s=60, label="LLM Proactive", zorder=3, marker="D", alpha=0.8)
        ax.plot(pro_times, pro_vals, color=PINK, alpha=0.3, linewidth=1)

    ax.axhline(y=70, color=TEXT, linestyle="--", alpha=0.3)
    ax.text(2, 72, "Good", fontsize=8, color=TEXT, alpha=0.5)
    ax.set_xlabel("Simulation Time (ticks)")
    ax.set_ylabel("Decision Score (0-100)")
    ax.set_title("20-Node Simulation: Decision Score Timeline", fontsize=14, fontweight="bold", color=ACCENT)
    ax.set_ylim(0, 105)
    ax.legend()
    ax.grid(alpha=0.15, color=TEXT)
    save_chart(fig, "7_score_timeline_20")

fig, axes = plt.subplots(2, 2, figsize=(12, 8))

ax = axes[0, 0]
timeout_rates = [v["timeout_rate"] for v in valid.values()]
bars = ax.bar(labels, timeout_rates, color=RED, alpha=0.9, width=0.5)
for bar, val in zip(bars, timeout_rates):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
            f"{val:.1f}%", ha="center", fontsize=10, color=RED, fontweight="bold")
ax.set_title("Timeout Rate", fontweight="bold", color=RED)
ax.set_ylabel("Rate (%)")
ax.set_ylim(0, max(max(timeout_rates) * 1.5, 10))
ax.grid(axis="y", alpha=0.15, color=TEXT)

ax = axes[0, 1]
elections = [v["leader_elections"] for v in valid.values()]
bars = ax.bar(labels, elections, color=PURPLE, alpha=0.9, width=0.5)
for bar, val in zip(bars, elections):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
            str(val), ha="center", fontsize=11, color=PURPLE, fontweight="bold")
ax.set_title("Leader Elections Triggered", fontweight="bold", color=PURPLE)
ax.set_ylabel("Count")
ax.set_ylim(0, max(max(elections) * 1.5, 3))
ax.grid(axis="y", alpha=0.15, color=TEXT)

ax = axes[1, 0]
quorum_down = [v["quorum_unavailable"] for v in valid.values()]
bars = ax.bar(labels, quorum_down, color=ORANGE, alpha=0.9, width=0.5)
for bar, val in zip(bars, quorum_down):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
            f"{val:.1f}", ha="center", fontsize=11, color=ORANGE, fontweight="bold")
ax.set_title("Quorum Unavailable (ticks)", fontweight="bold", color=ORANGE)
ax.set_ylabel("Ticks")
ax.set_ylim(0, max(max(quorum_down) * 1.5, 2))
ax.grid(axis="y", alpha=0.15, color=TEXT)

ax = axes[1, 1]
splits = [v["split_brain"] for v in valid.values()]
bars = ax.bar(labels, splits, color=PINK, alpha=0.9, width=0.5)
for bar, val in zip(bars, splits):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
            str(val), ha="center", fontsize=11, color=PINK, fontweight="bold")
ax.set_title("Split-Brain Events", fontweight="bold", color=PINK)
ax.set_ylabel("Count")
ax.set_ylim(0, max(max(splits) * 1.5, 2))
ax.grid(axis="y", alpha=0.15, color=TEXT)

fig.suptitle("20-Node Simulation: Cluster Health & Correctness Metrics", fontsize=15, fontweight="bold", color=ACCENT, y=1.02)
fig.tight_layout()
save_chart(fig, "8_health_dashboard_20")

fig, ax = plt.subplots(figsize=(8, 5))
source_counts = {}
for v in valid.values():
    for s in v["scores"]:
        src = s["source"]
        source_counts[src] = source_counts.get(src, 0) + 1

tier_labels = []
tier_vals = []
tier_colors = []
source_map = {
    "rule": ("Tier 1\nRule Engine\n(conf ≥90%)", ORANGE),
    "llm": ("Tier 2/3\nLLM Decisions\n(conf <90%)", GREEN),
    "llm_async": ("Tier 2/3\nLLM Async", GREEN),
    "llm_proactive": ("Proactive\nLLM\nOptimization", PINK),
    "llm_shed": ("Load\nShedding", RED),
    "llm_predictive": ("Predictive\nPrevention", PURPLE),
}

for src, (label, color) in source_map.items():
    if src in source_counts:
        tier_labels.append(label)
        tier_vals.append(source_counts[src])
        tier_colors.append(color)

if tier_labels:
    bars = ax.bar(tier_labels, tier_vals, color=tier_colors, alpha=0.9, width=0.5)
    for bar, val in zip(bars, tier_vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                str(val), ha="center", fontsize=12, fontweight="bold", color=bar.get_facecolor())
    ax.set_ylabel("Number of Decisions")
    ax.set_title("20-Node Simulation: Decision Routing", fontsize=14, fontweight="bold", color=ACCENT)
    ax.grid(axis="y", alpha=0.15, color=TEXT)
save_chart(fig, "9_decision_routing_20")


try:
    from decision_memory import memory

    mem_stats = memory.get_summary_stats()
    if mem_stats.get("total_entries", 0) > 0 and "by_action" in mem_stats:
        fig, ax = plt.subplots(figsize=(10, 5))
        actions = list(mem_stats["by_action"].keys())
        avg_scores = [mem_stats["by_action"][a]["avg_score"] for a in actions]
        resolved_pcts = [mem_stats["by_action"][a]["resolved_pct"] for a in actions]

        action_labels_mem = [a.replace("_", "\n") for a in actions]
        x_mem = np.arange(len(actions))
        width_mem = 0.35

        bars1 = ax.bar(x_mem - width_mem / 2, avg_scores, width_mem, label="Avg Score", color=ACCENT, alpha=0.9)
        bars2 = ax.bar(x_mem + width_mem / 2, resolved_pcts, width_mem, label="Resolved %", color=GREEN, alpha=0.9)

        for bar in bars1:
            if bar.get_height() > 3:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                        f"{bar.get_height():.0f}", ha="center", fontsize=8, color=ACCENT)
        for bar in bars2:
            if bar.get_height() > 3:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                        f"{bar.get_height():.0f}%", ha="center", fontsize=8, color=GREEN)

        ax.set_ylabel("Score / Percentage")
        ax.set_title(
            f"Cross-Run Learning Memory — {mem_stats['total_entries']} Decisions Stored",
            fontsize=14,
            fontweight="bold",
            color=ACCENT,
        )
        ax.set_xticks(x_mem)
        ax.set_xticklabels(action_labels_mem, fontsize=9)
        ax.set_ylim(0, 110)
        ax.legend()
        ax.grid(axis="y", alpha=0.15, color=TEXT)
        save_chart(fig, "10_cross_run_learning_20")
except Exception as e:
    print(f"  ⚠ Cross-run learning chart skipped: {e}")

print(f"\n{'=' * 50}")
print(f"  ✓ ALL 20-NODE CHARTS SAVED TO: {CHART_DIR}/")
print(f"{'=' * 50}")
print("\nCharts generated:")
for f in sorted(os.listdir(CHART_DIR)):
    if f.endswith(".png"):
        print(f"  📊 {f}")