
import json
import os
from pathlib import Path

HISTORY_FILE = Path(__file__).resolve().parent / "decision_history.json"
MAX_HISTORY_ENTRIES = 200
MAX_CONTEXT_ENTRIES = 5  


class DecisionMemory:
    """
    Persistent memory of past decisions and their outcomes.
    Enables cross-run learning by storing what worked and what didn't.
    """

    def __init__(self, history_file=None):
        self._file = history_file or HISTORY_FILE
        self._history = []
        self._load()

    def _load(self):
        """Load decision history from disk."""
        if os.path.exists(self._file):
            try:
                with open(self._file, "r") as f:
                    self._history = json.load(f)
                # Trim to max size
                if len(self._history) > MAX_HISTORY_ENTRIES:
                    self._history = self._history[-MAX_HISTORY_ENTRIES:]
            except (json.JSONDecodeError, IOError):
                self._history = []

    def save(self):
        """Persist decision history to disk."""
        try:
            
            if len(self._history) > MAX_HISTORY_ENTRIES:
                self._history = self._history[-MAX_HISTORY_ENTRIES:]
            with open(self._file, "w") as f:
                json.dump(self._history, f, indent=2)
        except IOError as e:
            print(f"[MEMORY] Warning: could not save decision history: {e}")

    def record_outcome(self, failure_type, node_role, action, source,
                       score, resolved, mttr, cluster_size,
                       concurrent_failures=0, health_at_decision=0):
        """
        Record the outcome of a decision for future learning.
        Called after scoring is complete.
        """
        entry = {
            "failure_type": failure_type,
            "node_role": node_role,
            "action": action,
            "source": source,
            "score": round(score, 1) if score else 0,
            "resolved": resolved,
            "mttr": round(mttr, 1) if mttr else None,
            "cluster_size": cluster_size,
            "concurrent_failures": concurrent_failures,
            "health_at_decision": round(health_at_decision, 0),
        }
        self._history.append(entry)

    def get_relevant_context(self, failure_type, node_role=None, limit=None):
        """
        Retrieve past decisions relevant to the current failure.
        Returns a formatted string for LLM prompt injection.
        """
        limit = limit or MAX_CONTEXT_ENTRIES

        relevant = [
            h for h in self._history
            if h["failure_type"] == failure_type
        ]

        
        if node_role and len(relevant) > limit:
            same_role = [h for h in relevant if h["node_role"] == node_role]
            if same_role:
                relevant = same_role

       
        relevant.sort(key=lambda h: h.get("score", 0), reverse=True)
        relevant = relevant[:limit]

        if not relevant:
            return ""

        lines = ["=== LEARNING FROM PAST SIMULATIONS ==="]
        for h in relevant:
            resolved_tag = "RESOLVED" if h["resolved"] else "UNRESOLVED"
            mttr_val = h["mttr"]
            concurrent_val = h["concurrent_failures"]
            mttr_text = f", MTTR={mttr_val} ticks" if mttr_val else ""
            concurrent_text = f", concurrent_failures={concurrent_val}" if concurrent_val else ""
            lines.append(
                f"- {h['failure_type']} on {h['node_role']}: "
                f"action={h['action']} (by {h['source']}) → "
                f"score={h['score']}/100, {resolved_tag}"
                f"{mttr_text}{concurrent_text}"
            )

        
        best = relevant[0] if relevant else None
        worst = min(relevant, key=lambda h: h.get("score", 0)) if relevant else None
        if best and worst and best != worst:
            lines.append(
                f"\nBest outcome: {best['action']} (score={best['score']}). "
                f"Worst outcome: {worst['action']} (score={worst['score']}). "
                f"Learn from past successes."
            )

        return "\n".join(lines)

    def get_summary_stats(self):
        """Get aggregate stats from history for reporting."""
        if not self._history:
            return {"total_entries": 0}

        by_action = {}
        for h in self._history:
            action = h["action"]
            if action not in by_action:
                by_action[action] = {"count": 0, "total_score": 0, "resolved": 0}
            by_action[action]["count"] += 1
            by_action[action]["total_score"] += h.get("score", 0)
            if h.get("resolved"):
                by_action[action]["resolved"] += 1

        summary = {}
        for action, data in by_action.items():
            n = data["count"]
            summary[action] = {
                "count": n,
                "avg_score": round(data["total_score"] / n, 1),
                "resolved_pct": round(data["resolved"] / n * 100, 1),
            }

        return {
            "total_entries": len(self._history),
            "by_action": summary,
        }

    def reset(self):
        """Clear in-memory history (does NOT delete file)."""
        self._history = []

    def clear_file(self):
        """Delete the persistent history file."""
        self._history = []
        if os.path.exists(self._file):
            os.remove(self._file)


memory = DecisionMemory()
