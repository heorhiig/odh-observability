/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// --- helpers -----------------------------------------------------------------

func probeGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: probeAPIGroup, Version: "v1", Kind: probeKind}
}

// gaugeValue reads the current float64 value of a prometheus.Gauge/Counter via
// the dto.Metric Write method so we don't import the internal registry directly.
func gaugeValue(t *testing.T, g interface {
	Write(m *dto.Metric) error
}) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		t.Fatalf("reading metric: %v", err)
	}
	if m.GetGauge() != nil {
		return m.GetGauge().GetValue()
	}
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return 0
}

// newTestProbe returns an AdmissionProbe whose retryTimeout is short so failure
// tests don't take 10 seconds.
func newTestProbe(c client.Client, namespace string) *AdmissionProbe {
	p := NewAdmissionProbe(c, namespace, time.Minute)
	p.retryTimeout = 200 * time.Millisecond
	return p
}

// --- mergeAnnotation ---------------------------------------------------------

func TestMergeAnnotation_NilMap(t *testing.T) {
	got := mergeAnnotation(nil, "k", "v")
	if got["k"] != "v" {
		t.Errorf("expected k=v, got %v", got)
	}
}

func TestMergeAnnotation_ExistingMap(t *testing.T) {
	ann := map[string]string{"existing": "yes"}
	got := mergeAnnotation(ann, "new-key", "new-val")
	if got["existing"] != "yes" {
		t.Errorf("existing key lost: %v", got)
	}
	if got["new-key"] != "new-val" {
		t.Errorf("expected new-key=new-val, got %v", got)
	}
}

func TestMergeAnnotation_Overwrite(t *testing.T) {
	ann := map[string]string{"key": "old"}
	got := mergeAnnotation(ann, "key", "new")
	if got["key"] != "new" {
		t.Errorf("expected key=new, got %v", got)
	}
}

// --- runProbe: success path --------------------------------------------------

// simulatingWebhook wraps a fake client and adds monitoring.opendatahub.io/scrape=true
// on every Create/Update, mimicking what the mutating webhook does on the real API server.
func webhookMutatingClient(t *testing.T) client.Client { //nolint:ireturn // test helper; concrete type not exported by controller-runtime/fake
	t.Helper()
	s := newTestScheme()
	// Register the probe GVK so the fake client can store it (newTestScheme already
	// registers the RHOBS PodMonitor GVK used by the probe, but register explicitly
	// in case newTestScheme ever changes).
	s.AddKnownTypeWithName(probeGVK(), &unstructured.Unstructured{})
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: probeAPIGroup, Version: "v1", Kind: probeKind + "List"},
		&unstructured.UnstructuredList{},
	)

	addScrapeLabel := func(obj client.Object) {
		labels := obj.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["monitoring.opendatahub.io/scrape"] = "true"
		obj.SetLabels(labels)
	}

	return fake.NewClientBuilder().
		WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				addScrapeLabel(obj)
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				addScrapeLabel(obj)
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
}

func TestRunProbe_WebhookMutates_FirstCreate(t *testing.T) {
	c := webhookMutatingClient(t)
	p := newTestProbe(c, "test-ns")

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)

	p.runProbe(context.Background())

	afterFailures := gaugeValue(t, webhookProbeFailuresTotal)
	if afterFailures != beforeFailures {
		t.Errorf("expected no new failures, got %v new failures", afterFailures-beforeFailures)
	}

	// Success timestamp should be non-zero.
	ts := gaugeValue(t, webhookProbeLastSuccessTimestamp)
	if ts == 0 {
		t.Error("expected non-zero last success timestamp after successful probe")
	}
}

func TestRunProbe_WebhookMutates_SubsequentUpdate(t *testing.T) {
	c := webhookMutatingClient(t)
	p := newTestProbe(c, "test-ns")

	// First run creates the probe PodMonitor.
	p.runProbe(context.Background())

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)

	// Second run updates it (exercises the Update branch of runProbe).
	p.runProbe(context.Background())

	afterFailures := gaugeValue(t, webhookProbeFailuresTotal)
	if afterFailures != beforeFailures {
		t.Errorf("expected no new failures on second run, got %v new failures", afterFailures-beforeFailures)
	}
}

// --- runProbe: failure path --------------------------------------------------

// noMutationClient returns a plain fake client that stores objects as-is (no webhook simulation).
func noMutationClient(t *testing.T) client.Client { //nolint:ireturn // test helper; concrete type not exported by controller-runtime/fake
	t.Helper()
	s := newTestScheme()
	s.AddKnownTypeWithName(probeGVK(), &unstructured.Unstructured{})
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: probeAPIGroup, Version: "v1", Kind: probeKind + "List"},
		&unstructured.UnstructuredList{},
	)
	return fake.NewClientBuilder().WithScheme(s).Build() //nolint:wrapcheck
}

func TestRunProbe_WebhookDoesNotMutate(t *testing.T) {
	c := noMutationClient(t)
	p := newTestProbe(c, "test-ns")

	beforeFailures := gaugeValue(t, webhookProbeFailuresTotal)

	p.runProbe(context.Background())

	afterFailures := gaugeValue(t, webhookProbeFailuresTotal)
	if afterFailures == beforeFailures {
		t.Error("expected at least one failure when webhook does not mutate, got none")
	}
}

// --- Start: context cancellation --------------------------------------------

func TestStart_ContextCancellation(t *testing.T) {
	c := webhookMutatingClient(t)
	p := NewAdmissionProbe(c, "test-ns", 100*time.Millisecond)
	p.retryTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	// Start should return promptly after the context is cancelled.
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}
