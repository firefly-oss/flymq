/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tracing

import (
	"context"
	"testing"

	"flymq/internal/config"
)

func TestNewTracer(t *testing.T) {
	cfg := &config.TracingConfig{
		Enabled:    true,
		SampleRate: 1.0,
	}
	tracer := NewTracer(cfg)
	if tracer == nil {
		t.Fatal("Expected non-nil tracer")
	}
}

func TestStartSpanDisabled(t *testing.T) {
	cfg := &config.TracingConfig{
		Enabled:    false,
		SampleRate: 1.0,
	}
	tracer := NewTracer(cfg)

	ctx, span := tracer.StartSpan(context.Background(), "test-span", SpanKindInternal)
	if span != nil {
		t.Error("Expected nil span when tracing is disabled")
	}
	if ctx == nil {
		t.Error("Expected non-nil context")
	}
}

func TestStartSpanEnabled(t *testing.T) {
	cfg := &config.TracingConfig{
		Enabled:    true,
		SampleRate: 1.0,
	}
	tracer := NewTracer(cfg)

	ctx, span := tracer.StartSpan(context.Background(), "test-span", SpanKindServer)
	if span == nil {
		t.Fatal("Expected non-nil span")
	}
	if ctx == nil {
		t.Fatal("Expected non-nil context")
	}

	if span.Name != "test-span" {
		t.Errorf("Expected span name 'test-span', got %s", span.Name)
	}
	if span.Kind != SpanKindServer {
		t.Errorf("Expected kind Server, got %s", span.Kind)
	}
	if span.TraceID == "" {
		t.Error("Expected non-empty trace ID")
	}
	if span.SpanID == "" {
		t.Error("Expected non-empty span ID")
	}
}

func TestEndSpan(t *testing.T) {
	cfg := &config.TracingConfig{Enabled: true, SampleRate: 1.0}
	tracer := NewTracer(cfg)

	_, span := tracer.StartSpan(context.Background(), "test-span", SpanKindInternal)
	tracer.EndSpan(span)

	if span.EndTime.IsZero() {
		t.Error("Expected EndTime to be set")
	}
	if span.Status != StatusOK {
		t.Errorf("Expected status OK, got %s", span.Status)
	}
}

func TestSetSpanStatus(t *testing.T) {
	cfg := &config.TracingConfig{Enabled: true, SampleRate: 1.0}
	tracer := NewTracer(cfg)

	_, span := tracer.StartSpan(context.Background(), "test-span", SpanKindInternal)
	tracer.SetSpanStatus(span, StatusError, "something went wrong")

	if span.Status != StatusError {
		t.Errorf("Expected status Error, got %s", span.Status)
	}
	if span.Attributes["error.message"] != "something went wrong" {
		t.Error("Expected error message in attributes")
	}
}

func TestSetSpanAttribute(t *testing.T) {
	cfg := &config.TracingConfig{Enabled: true, SampleRate: 1.0}
	tracer := NewTracer(cfg)

	_, span := tracer.StartSpan(context.Background(), "test-span", SpanKindInternal)
	tracer.SetSpanAttribute(span, "topic", "orders")

	if span.Attributes["topic"] != "orders" {
		t.Errorf("Expected attribute 'topic'='orders', got %s", span.Attributes["topic"])
	}
}

func TestAddSpanEvent(t *testing.T) {
	cfg := &config.TracingConfig{Enabled: true, SampleRate: 1.0}
	tracer := NewTracer(cfg)

	_, span := tracer.StartSpan(context.Background(), "test-span", SpanKindInternal)
	tracer.AddSpanEvent(span, "message_received", map[string]string{"size": "1024"})

	if len(span.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(span.Events))
	}
	if span.Events[0].Name != "message_received" {
		t.Errorf("Expected event name 'message_received', got %s", span.Events[0].Name)
	}
}

func TestSpanContext(t *testing.T) {
	cfg := &config.TracingConfig{Enabled: true, SampleRate: 1.0}
	tracer := NewTracer(cfg)

	ctx, span := tracer.StartSpan(context.Background(), "parent", SpanKindServer)

	// Start child span
	_, childSpan := tracer.StartSpan(ctx, "child", SpanKindInternal)

	if childSpan.ParentID != span.SpanID {
		t.Errorf("Expected parent ID %s, got %s", span.SpanID, childSpan.ParentID)
	}
	if childSpan.TraceID != span.TraceID {
		t.Errorf("Expected same trace ID")
	}
}

func TestInjectExtractTraceContext(t *testing.T) {
	cfg := &config.TracingConfig{Enabled: true, SampleRate: 1.0}
	tracer := NewTracer(cfg)

	_, span := tracer.StartSpan(context.Background(), "test", SpanKindProducer)

	headers := make(map[string]string)
	tracer.InjectTraceContext(span, headers)

	if headers["traceparent"] == "" {
		t.Error("Expected traceparent header")
	}

	tc := tracer.ExtractTraceContext(headers)
	if tc == nil {
		t.Fatal("Expected non-nil trace context")
	}
	if tc.TraceID != span.TraceID {
		t.Errorf("Expected trace ID %s, got %s", span.TraceID, tc.TraceID)
	}
}
