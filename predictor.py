class HealthPredictor:
    """
    Tracks node health over time and predicts
    which nodes are likely to fail soon.
    """
    def __init__(self):
        self._history = {}         
        self._alert_state = {}     

    def record(self, node_id, health, current_time):
        """Record a health reading for a node."""
        if node_id not in self._history:
            self._history[node_id] = []
        self._history[node_id].append((current_time, health))

        
        self._history[node_id] = self._history[node_id][-10:]

    def predict(self, node_id):
        """
        Predict if a node will fail soon based on health trend.
        Returns (will_fail, ticks_until_failure, confidence, slope)
        """
        history = self._history.get(node_id, [])
        if len(history) < 3:
            return False, None, 0, 0

        times = [h[0] for h in history]
        healths = [h[1] for h in history]

       
        n = len(times)
        sum_t = sum(times)
        sum_h = sum(healths)
        sum_th = sum(t * h for t, h in zip(times, healths))
        sum_t2 = sum(t * t for t in times)

        denom = n * sum_t2 - sum_t * sum_t
        if denom == 0:
            return False, None, 0, 0

        slope = (n * sum_th - sum_t * sum_h) / denom

        
        if slope < -2:
            current_health = healths[-1]
            if current_health <= 0:
                return False, None, 0, slope

            
            ticks_until_zero = -current_health / slope
            confidence = min(95, int(abs(slope) * 10))
            return True, round(ticks_until_zero, 1), confidence, slope

        return False, None, 0, slope

    def _reset_node_alert_state_if_recovered(self, node_id, node):
        """
        Clear alert suppression when a node looks recovered enough that
        future degradation should count as a new incident.
        """
        state = self._alert_state.get(node_id)
        if not state:
            return

        if node.status == "healthy" and node.health >= 80:
            self._alert_state.pop(node_id, None)

    def _should_emit_alert(self, node_id, node, current_time, ticks, confidence, slope):
        """
        Suppress repetitive alerts for the same ongoing degradation.
        Re-alert only if risk got meaningfully worse or enough time passed.
        """
        state = self._alert_state.get(node_id)

        if state is None:
            self._alert_state[node_id] = {
                "last_alert_time": current_time,
                "last_ticks": ticks,
                "last_confidence": confidence,
                "last_health": node.health,
                "last_slope": slope,
            }
            return True

        time_since = current_time - state["last_alert_time"]

       
        if ticks <= max(1.0, state["last_ticks"] - 2.0):
            self._alert_state[node_id] = {
                "last_alert_time": current_time,
                "last_ticks": ticks,
                "last_confidence": confidence,
                "last_health": node.health,
                "last_slope": slope,
            }
            return True

        
        if confidence >= state["last_confidence"] + 15:
            self._alert_state[node_id] = {
                "last_alert_time": current_time,
                "last_ticks": ticks,
                "last_confidence": confidence,
                "last_health": node.health,
                "last_slope": slope,
            }
            return True

        
        if time_since >= 5:
            self._alert_state[node_id] = {
                "last_alert_time": current_time,
                "last_ticks": ticks,
                "last_confidence": confidence,
                "last_health": node.health,
                "last_slope": slope,
            }
            return True

        return False

    def check_all_nodes(self, network, current_time):
        """
        Check all nodes for predicted failures.
        Returns list of alerts.
        """
        alerts = []

        for node_id, node in network.nodes.items():
            
            self.record(node_id, node.health, current_time)

            
            self._reset_node_alert_state_if_recovered(node_id, node)

            
            if node.status == "failed":
                continue

            
            will_fail, ticks, confidence, slope = self.predict(node_id)

            if will_fail and ticks and ticks < 12:
                if self._should_emit_alert(
                    node_id, node, current_time, ticks, confidence, slope
                ):
                    alerts.append({
                        "node_id": node_id,
                        "ticks": ticks,
                        "confidence": confidence,
                        "health": node.health,
                        "status": node.status
                    })
            else:
                
                
                if node.status == "healthy" and node.health >= 80:
                    self._alert_state.pop(node_id, None)

        return alerts


predictor = HealthPredictor()
