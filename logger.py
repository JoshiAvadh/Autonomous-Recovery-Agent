import os
from datetime import datetime


class SimulationLogger:
    def __init__(self, filename=None):
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename  = f"simulation_{timestamp}.txt"
        self.filename = filename
        self.entries  = []

        self._write(f"{'='*60}")
        self._write(f"  DISTRIBUTED SYSTEM SIMULATION LOG")
        self._write(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._write(f"{'='*60}\n")

    def log(self, message, level="info"):
        tag = {
            "info":    "[ INFO ]",
            "error":   "[ERROR ]",
            "success": "[  OK  ]",
            "warn":    "[ WARN ]",
            "llm":     "[ LLM  ]",
        }.get(level, "[ INFO ]")
        entry = f"{tag} {message}"
        self.entries.append(entry)
        self._write(entry)

    def log_cluster_status(self, sim_time, network):
        self._write(f"\n── Cluster at t={sim_time:.0f} ───────────────────────────")
        for node in network.nodes.values():
            bar   = "█" * (int(node.load) // 10) + "░" * (10 - int(node.load) // 10)
            emoji = "✓" if node.status == "healthy" else "⚠" if node.status == "degraded" else "✕"
            self._write(
                f"  {emoji} {node.id} ({node.role:8}) | {bar} {node.load:3.0f}% | health={node.health:.0f}%"
            )
        stats = network.get_stats()
        self._write(
            f"  Messages: {stats['total_messages']} sent | "
            f"{stats['delivered']} delivered | "
            f"{stats['dropped']} dropped | "
            f"success={stats['success_rate']}"
        )

    def log_llm_decision(self, sim_time, failure_type, target, decision):
        override_marker = " *** LLM OVERRIDE ***" if "[LLM OVERRIDE]" in decision else ""
        self._write(f"\n-- LLM Decision at t={sim_time:.1f}{override_marker} ---------")
        self._write(f"  Failure : {failure_type} on {target}")
        self._write(f"  Decision:")
        for line in decision.strip().split("\n"):
            self._write(f"    {line}")

    def log_llm_optimization(self, sim_time, suggestion):
        self._write(f"\n── LLM Optimization at t={sim_time:.1f} ──────────────────")
        for line in suggestion.strip().split("\n"):
            self._write(f"  {line}")

    def log_metrics_summary(self, metrics, avg_mttr, resolved, total, net_stats):
        self._write(f"\n{'=' * 50}")
        self._write("  SIMULATION METRICS")
        self._write(f"{'=' * 50}")
        self._write(f"  Total LLM decisions:    {metrics['total_decisions']}")
        self._write(f"  LLM novel overrides:    {metrics['llm_overrides']}")
        self._write(f"  Rule echoes:            {metrics['rule_echoes']}")
        if avg_mttr:
            self._write(f"  Avg MTTR:               {avg_mttr} ticks")
        self._write(f"  Failures resolved:      {resolved}/{total}")
        self._write(f"  Message delivery:       {net_stats['success_rate']}")
        self._write(f"{'=' * 50}")

    def log_final_report(self, report, failure_history, network):
        self._write(f"\n{'='*60}")
        self._write(f"  FINAL REPORT")
        self._write(f"{'='*60}")

        
        self._write("\n── LLM Analysis ───────")
        for line in report.strip().split("\n"):
            self._write(f"  {line}")

        
        self._write("\n── Failure History ──────")
        mttr_values = []
        for ev in failure_history:
            if ev.resolved and ev.resolved_at:
                mttr = ev.resolved_at - ev.time
                mttr_values.append(mttr)
                self._write(
                    f"  [{ev.time:5.1f}] {ev.failure_type:20} on {ev.target:12} "
                    f"(resolved@t={ev.resolved_at:.1f}, MTTR={mttr:.1f})"
                )
            else:
                self._write(
                    f"  [{ev.time:5.1f}] {ev.failure_type:20} on {ev.target:12} (ACTIVE)"
                )

        total    = len(failure_history)
        active   = sum(1 for e in failure_history if not e.resolved)
        resolved = total - active
        self._write(f"\n  Total: {total} | Active: {active} | Resolved: {resolved}")
        if mttr_values:
            avg_mttr = sum(mttr_values) / len(mttr_values)
            self._write(f"\n  Avg MTTR: {avg_mttr:.1f} ticks across {len(mttr_values)} resolved failures")

        
        self._write("\n── Final Node States ─────")
        for node in network.nodes.values():
            restored_tag = " [FULLY RESTORED]" if node.health >= 100 and node.status == "healthy" else ""
            self._write(
                f"  {node.id} ({node.role:8}): status={node.status:10} "
                f"health={node.health:.0f}% load={node.load:.0f}%{restored_tag}"
            )

        
        stats = network.get_stats()
        self._write("\n── Final Network Stats ──────")
        for k, v in stats.items():
            self._write(f"  {k}: {v}")

        self._write(f"\n  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self._write(f"{'='*60}")
        print(f"\n✓ Log saved to: {self.filename}")

    def log_scorer_report(self, report_text):
        """Log the decision scoring comparison report to file only (already printed)."""
        self._write_file_only(f"\n── Decision Scoring ─────────")
        for line in report_text.strip().split("\n"):
            self._write_file_only(f"  {line}")

    def _write(self, text):
        print(text)
        with open(self.filename, "a") as f:
            f.write(text + "\n")

    def _write_file_only(self, text):
        """Write to file without printing (content already on console)."""
        with open(self.filename, "a") as f:
            f.write(text + "\n")
