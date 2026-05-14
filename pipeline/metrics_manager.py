from collections import Counter


class MetricsManager:

    def __init__(self):

        self.total_events = 0
        self.total_alerts = 0

        self.event_types = Counter()
        self.districts = Counter()

        self.total_price = 0

    def process_event(self, event):

        self.total_events += 1

        event_type = event["event_type"]

        self.event_types[event_type] += 1

        data = event["data"]

        district = data["district"]

        self.districts[district] += 1

        self.total_price += data["price"]

    def process_alert(self):

        self.total_alerts += 1

    def get_metrics(self):

        avg_price = 0

        if self.total_events > 0:
            avg_price = round(
                self.total_price / self.total_events,
                2
            )

        return {
            "total_events": self.total_events,
            "total_alerts": self.total_alerts,
            "avg_price": avg_price,
            "top_event_types": dict(self.event_types),
            "top_districts": dict(self.districts)
        }
