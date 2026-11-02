"""LiveFailureEvent — duck-types failures.FailureEvent (failures.py:19).

The reused decision code takes a failure event and reads .failure_type,
.target, .time, .resolved, .resolved_at, and calls .resolve(). Matching that
shape is what lets rule_based.py and llm_agent.py be imported unmodified.

Time is SECONDS here, not the simulator's ticks. scorer.py's
`score_after = current_time + 5.0` therefore means five seconds, which is the
intended meaning — but the same constant is why absolute scores do not
transfer between the two systems (§8.3).
"""

import time


class LiveFailureEvent:
    def __init__(self, failure_type, target, when=None, diagnosis=None,
                 incident_id=None):
        self.failure_type = failure_type
        self.target = target
        self.time = when if when is not None else time.time()
        self.resolved = False
        self.resolved_at = None
        # Carried so the intent can quote the evidence the LLM should see and
        # the evaluator can group by incident without reversing a hash (§6.1).
        self.diagnosis = diagnosis
        self.incident_id = incident_id

    def resolve(self, resolved_at):
        self.resolved = True
        self.resolved_at = resolved_at

    def __repr__(self):
        return "<LiveFailureEvent %s on %s @%.1f%s>" % (
            self.failure_type, self.target, self.time,
            " RESOLVED" if self.resolved else "")


def from_diagnosis(diagnosis, incident):
    return LiveFailureEvent(
        failure_type=diagnosis.failure_type,
        target=diagnosis.target,
        when=diagnosis.observed_at,
        diagnosis=diagnosis.to_dict(),
        incident_id=incident.incident_id if incident else None,
    )
