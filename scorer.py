
import time as _time


class DecisionScorer:
    """
    Scores each recovery decision based on measurable outcomes.
    Called after a decision is executed and again when the outcome is known.
    """

    def __init__(self):
        self._pending = {}     
        self._scores  = []     
        self._id_counter = 0

    def register_decision(self, current_time, action, target, source,
                          failure_type, network, rule_suggestion=None):
        """
        Register a decision when it's made. Returns a decision_id
        for later scoring.
        """
        self._id_counter += 1
        decision_id = self._id_counter

        
        healthy_count  = sum(1 for n in network.nodes.values() if n.status == "healthy")
        degraded_count = sum(1 for n in network.nodes.values() if n.status == "degraded")
        failed_count   = sum(1 for n in network.nodes.values() if n.status == "failed")
        net_stats      = network.get_stats()

        self._pending[decision_id] = {
            "id":               decision_id,
            "time":             current_time,
            "action":           action,
            "target":           target,
            "source":           source,           
            "failure_type":     failure_type,
            "rule_suggestion":  rule_suggestion,   
            "diverged":         (rule_suggestion is not None
                                 and action != rule_suggestion),
            
            "pre_healthy":      healthy_count,
            "pre_degraded":     degraded_count,
            "pre_failed":       failed_count,
            "pre_delivery_rate": net_stats["success_rate"],
            "pre_total_msgs":   net_stats["total_messages"],
            "score_after":      current_time + 5.0,  
        }
        return decision_id

    def is_ready_to_score(self, decision_id, current_time):
        """Check if enough time has passed to score this decision."""
        ctx = self._pending.get(decision_id)
        if ctx is None:
            return False
        return current_time >= ctx.get("score_after", 0)

    def score_outcome(self, decision_id, current_time, network, resolved=True):
        """
        Score the decision based on what happened after it was executed.
        Call this ~5 ticks after the decision (from the feedback loop).
        """
        if decision_id not in self._pending:
            return None

        ctx = self._pending.pop(decision_id)

        
        healthy_count  = sum(1 for n in network.nodes.values() if n.status == "healthy")
        degraded_count = sum(1 for n in network.nodes.values() if n.status == "degraded")
        failed_count   = sum(1 for n in network.nodes.values() if n.status == "failed")
        net_stats      = network.get_stats()

        
        mttr = current_time - ctx["time"]
        if resolved:
            
            speed_score = max(10, min(100, 120 - mttr * 10))
        else:
            speed_score = 0

       
        health_delta = healthy_count - ctx["pre_healthy"]
        if health_delta > 0:
            stability_score = 90 + min(10, health_delta * 5)
        elif health_delta == 0:
            stability_score = 70
        else:
            stability_score = max(0, 50 + health_delta * 15)

        
        new_failures = failed_count - ctx["pre_failed"]
        if new_failures <= 0:
            cascade_score = 100
        elif new_failures == 1:
            cascade_score = 40
        else:
            cascade_score = 0

        
        try:
            pre_rate  = float(ctx["pre_delivery_rate"].replace("%", ""))
            post_rate = float(net_stats["success_rate"].replace("%", ""))
            rate_delta = post_rate - pre_rate
            delivery_score = min(100, max(0, 70 + rate_delta * 3))
        except (ValueError, AttributeError):
            delivery_score = 50

        
        total_score = (
            speed_score     * 0.35 +
            stability_score * 0.25 +
            cascade_score   * 0.25 +
            delivery_score  * 0.15
        )

        record = {
            **ctx,
            "resolved":         resolved,
            "mttr":             mttr,
            "speed_score":      round(speed_score, 1),
            "stability_score":  round(stability_score, 1),
            "cascade_score":    round(cascade_score, 1),
            "delivery_score":   round(delivery_score, 1),
            "total_score":      round(total_score, 1),
            "post_healthy":     healthy_count,
            "post_degraded":    degraded_count,
            "post_failed":      failed_count,
            "scored_at":        current_time,
        }
        self._scores.append(record)
        return record

    def get_scores(self):
        """Return all scored decisions."""
        return list(self._scores)

    def get_summary(self):
        """
        Aggregate performance by source (rule vs llm vs proactive).
        Returns dict with per-source averages.
        """
        from collections import defaultdict
        by_source = defaultdict(list)
        for s in self._scores:
            by_source[s["source"]].append(s)

        summary = {}
        for source, records in by_source.items():
            n = len(records)
            if n == 0:
                continue
            summary[source] = {
                "count":             n,
                "avg_total_score":   round(sum(r["total_score"] for r in records) / n, 1),
                "avg_speed_score":   round(sum(r["speed_score"] for r in records) / n, 1),
                "avg_stability":     round(sum(r["stability_score"] for r in records) / n, 1),
                "avg_cascade":       round(sum(r["cascade_score"] for r in records) / n, 1),
                "avg_delivery":      round(sum(r["delivery_score"] for r in records) / n, 1),
                "avg_mttr":          round(sum(r["mttr"] for r in records) / n, 1),
                "resolved_pct":      round(sum(1 for r in records if r["resolved"]) / n * 100, 1),
                "diverged_count":    sum(1 for r in records if r.get("diverged")),
            }

        return summary

    def get_comparison_report(self):
        """
        Generate a head-to-head LLM vs Rule comparison string.
        """
        summary = self.get_summary()
        llm_data  = summary.get("llm", summary.get("llm_async", {}))
        rule_data = summary.get("rule", {})
        proactive_data = summary.get("llm_proactive", {})

        if not llm_data and not rule_data:
            return "No scored decisions to compare."

        lines = []
        lines.append("=" * 56)
        lines.append("  DECISION SCORING: LLM vs RULE ENGINE")
        lines.append("=" * 56)

        header = f"{'Metric':<28} {'LLM':<14} {'Rules':<14}"
        lines.append(header)
        lines.append("-" * 56)

        def _val(data, key, fmt=".1f"):
            v = data.get(key, "N/A")
            if isinstance(v, (int, float)):
                return f"{v:{fmt}}"
            return str(v)

        metrics = [
            ("Decisions scored",     "count",           "d"),
            ("Avg total score",      "avg_total_score",  ".1f"),
            ("Avg speed score",      "avg_speed_score",  ".1f"),
            ("Avg stability score",  "avg_stability",    ".1f"),
            ("Avg cascade score",    "avg_cascade",      ".1f"),
            ("Avg delivery score",   "avg_delivery",     ".1f"),
            ("Avg MTTR (ticks)",     "avg_mttr",         ".1f"),
            ("Resolved %",          "resolved_pct",     ".1f"),
            ("Diverged from rules",  "diverged_count",   "d"),
        ]

        for label, key, fmt in metrics:
            lv = _val(llm_data, key, fmt) if llm_data else "N/A"
            rv = _val(rule_data, key, fmt) if rule_data else "N/A"
            lines.append(f"  {label:<26} {lv:<14} {rv:<14}")

        
        if proactive_data:
            lines.append("")
            lines.append(f"  Proactive LLM optimizations: {proactive_data.get('count', 0)}")
            lines.append(f"  Avg proactive score: {proactive_data.get('avg_total_score', 'N/A')}")

       
        lines.append("")
        if llm_data and rule_data:
            llm_score  = llm_data.get("avg_total_score", 0)
            rule_score = rule_data.get("avg_total_score", 0)
            diff = llm_score - rule_score
            if diff > 5:
                lines.append(f"  ★ LLM outperforms rules by {diff:.1f} points")
            elif diff < -5:
                lines.append(f"  ★ Rules outperform LLM by {abs(diff):.1f} points")
            else:
                lines.append(f"  ★ LLM and rules perform similarly (delta={diff:.1f})")

        lines.append("=" * 56)
        return "\n".join(lines)

    def reset(self):
        self._pending.clear()
        self._scores.clear()
        self._id_counter = 0



scorer = DecisionScorer()
