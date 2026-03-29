package testsupport

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func AssertMetricGauge(tb testing.TB, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	tb.Helper()

	metric := LookupMetric(tb, families, name, labels)
	if metric.GetGauge().GetValue() != want {
		tb.Fatalf("%s labels=%v value = %v, want %v", name, labels, metric.GetGauge().GetValue(), want)
	}
}

func AssertMetricCounter(tb testing.TB, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	tb.Helper()

	metric := LookupMetric(tb, families, name, labels)
	if metric.GetCounter().GetValue() != want {
		tb.Fatalf("%s labels=%v value = %v, want %v", name, labels, metric.GetCounter().GetValue(), want)
	}
}

func AssertMetricHistogramCount(tb testing.TB, families []*dto.MetricFamily, name string, labels map[string]string, want uint64) {
	tb.Helper()

	metric := LookupMetric(tb, families, name, labels)
	if metric.GetHistogram().GetSampleCount() != want {
		tb.Fatalf("%s labels=%v sample_count = %d, want %d", name, labels, metric.GetHistogram().GetSampleCount(), want)
	}
}

func AssertMetricHistogramSum(tb testing.TB, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	tb.Helper()

	metric := LookupMetric(tb, families, name, labels)
	if metric.GetHistogram().GetSampleSum() != want {
		tb.Fatalf("%s labels=%v sample_sum = %v, want %v", name, labels, metric.GetHistogram().GetSampleSum(), want)
	}
}

func LookupMetric(tb testing.TB, families []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	tb.Helper()

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if MetricLabelsEqual(metric, labels) {
				return metric
			}
		}
	}
	tb.Fatalf("metric %s labels=%v not found", name, labels)
	return nil
}

func MetricLabelsEqual(metric *dto.Metric, labels map[string]string) bool {
	if len(metric.GetLabel()) != len(labels) {
		return false
	}
	for _, label := range metric.GetLabel() {
		if labels[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}
