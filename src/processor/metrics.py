"""CloudWatch custom metrics using the Embedded Metric Format (EMF).

Metrics are written as a JSON line to stdout. CloudWatch Logs extracts them
into real metrics asynchronously, so publishing adds no API calls and no
latency to the processing path.
"""

import json
import time

COUNT_METRICS = (
    "EventsReceived",
    "EventsProcessed",
    "DuplicatesSkipped",
    "ValidationErrors",
    "RetryableErrors",
    "ErrorLevelEvents",
)
LATENCY_METRICS = (
    "EndToEndLatencyMs",
    "BatchDurationMs",
)
ALL_METRICS = COUNT_METRICS + LATENCY_METRICS

# EMF allows at most 100 values per metric in one document.
MAX_VALUES_PER_METRIC = 100


class MetricsBatch:
    def __init__(self, namespace, service, emit=print):
        self.namespace = namespace
        self.service = service
        self._emit = emit
        self._reset()

    def _reset(self):
        self._counts = {name: 0 for name in COUNT_METRICS}
        self._values = {name: [] for name in LATENCY_METRICS}

    def count(self, name, amount=1):
        if name not in self._counts:
            raise KeyError(f"unknown count metric: {name}")
        self._counts[name] += amount

    def value(self, name, val):
        if name not in self._values:
            raise KeyError(f"unknown latency metric: {name}")
        self._values[name].append(float(val))
        if len(self._values[name]) >= MAX_VALUES_PER_METRIC:
            self.flush()

    def build_document(self):
        metrics = [{"Name": n, "Unit": "Count"} for n in COUNT_METRICS]
        doc = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self.namespace,
                        "Dimensions": [["Service"]],
                        "Metrics": metrics,
                    }
                ],
            },
            "Service": self.service,
        }
        doc.update(self._counts)
        for name, values in self._values.items():
            if values:
                metrics.append({"Name": name, "Unit": "Milliseconds"})
                doc[name] = values
        return doc

    def flush(self):
        self._emit(json.dumps(self.build_document()))
        self._reset()
