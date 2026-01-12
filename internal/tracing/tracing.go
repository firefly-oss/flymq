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

/*
Package tracing provides OpenTelemetry-compatible distributed tracing.

OVERVIEW:
=========
Distributed tracing tracks requests across service boundaries.
Each operation creates a span with timing and metadata.

SPAN HIERARCHY:
===============

	[Client Request]
	    └── [Produce Message]
	         ├── [Validate Schema]
	         ├── [Write to Log]
	         └── [Replicate to Followers]

TRACE CONTEXT:
==============
Trace context is propagated via message headers:
- traceparent: W3C Trace Context format
- tracestate: Vendor-specific state

EXPORTERS:
==========
Traces can be exported to:
- Jaeger
- Zipkin
- OTLP (OpenTelemetry Protocol)
- Console (for debugging)
*/
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"flymq/internal/config"
	"flymq/internal/logging"
)

// SpanKind represents the type of span.
type SpanKind string

const (
	SpanKindInternal SpanKind = "internal"
	SpanKindServer   SpanKind = "server"
	SpanKindClient   SpanKind = "client"
	SpanKindProducer SpanKind = "producer"
	SpanKindConsumer SpanKind = "consumer"
)

// SpanStatus represents the status of a span.
type SpanStatus string

const (
	StatusUnset SpanStatus = "unset"
	StatusOK    SpanStatus = "ok"
	StatusError SpanStatus = "error"
)

// Span represents a single trace span.
type Span struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	ParentID   string            `json:"parent_id,omitempty"`
	Name       string            `json:"name"`
	Kind       SpanKind          `json:"kind"`
	StartTime  time.Time         `json:"start_time"`
	EndTime    time.Time         `json:"end_time,omitempty"`
	Status     SpanStatus        `json:"status"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Events     []SpanEvent       `json:"events,omitempty"`
}

// SpanEvent represents an event within a span.
type SpanEvent struct {
	Name       string            `json:"name"`
	Timestamp  time.Time         `json:"timestamp"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// TraceContext holds trace propagation context.
type TraceContext struct {
	TraceID string
	SpanID  string
	Sampled bool
}

// Tracer provides tracing functionality.
type Tracer struct {
	mu         sync.RWMutex
	config     *config.TracingConfig
	spans      []*Span
	exporter   Exporter
	logger     *logging.Logger
	sampleRate float64
}

// Exporter interface for exporting spans.
type Exporter interface {
	Export(spans []*Span) error
	Shutdown() error
}

// NewTracer creates a new tracer.
func NewTracer(cfg *config.TracingConfig) *Tracer {
	return &Tracer{
		config:     cfg,
		spans:      make([]*Span, 0),
		logger:     logging.NewLogger("tracing"),
		sampleRate: cfg.SampleRate,
	}
}

// SetExporter sets the span exporter.
func (t *Tracer) SetExporter(exp Exporter) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.exporter = exp
}

// StartSpan starts a new span.
func (t *Tracer) StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, *Span) {
	if !t.config.Enabled {
		return ctx, nil
	}

	// Check sampling
	if !t.shouldSample() {
		return ctx, nil
	}

	span := &Span{
		TraceID:    t.generateTraceID(ctx),
		SpanID:     t.generateSpanID(),
		Name:       name,
		Kind:       kind,
		StartTime:  time.Now(),
		Status:     StatusUnset,
		Attributes: make(map[string]string),
		Events:     make([]SpanEvent, 0),
	}

	// Get parent span from context
	if parent := t.SpanFromContext(ctx); parent != nil {
		span.ParentID = parent.SpanID
		span.TraceID = parent.TraceID
	}

	return t.ContextWithSpan(ctx, span), span
}

// EndSpan ends a span.
func (t *Tracer) EndSpan(span *Span) {
	if span == nil {
		return
	}

	span.EndTime = time.Now()
	if span.Status == StatusUnset {
		span.Status = StatusOK
	}

	t.mu.Lock()
	t.spans = append(t.spans, span)
	t.mu.Unlock()
}

// SetSpanStatus sets the status of a span.
func (t *Tracer) SetSpanStatus(span *Span, status SpanStatus, message string) {
	if span == nil {
		return
	}
	span.Status = status
	if message != "" {
		span.Attributes["error.message"] = message
	}
}

// SetSpanAttribute sets an attribute on a span.
func (t *Tracer) SetSpanAttribute(span *Span, key, value string) {
	if span == nil {
		return
	}
	span.Attributes[key] = value
}

// AddSpanEvent adds an event to a span.
func (t *Tracer) AddSpanEvent(span *Span, name string, attrs map[string]string) {
	if span == nil {
		return
	}
	span.Events = append(span.Events, SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attrs,
	})
}

// Flush exports all pending spans.
func (t *Tracer) Flush() error {
	t.mu.Lock()
	spans := t.spans
	t.spans = make([]*Span, 0)
	t.mu.Unlock()

	if len(spans) == 0 || t.exporter == nil {
		return nil
	}

	return t.exporter.Export(spans)
}

// Shutdown shuts down the tracer.
func (t *Tracer) Shutdown() error {
	if err := t.Flush(); err != nil {
		t.logger.Error("Failed to flush spans", "error", err)
	}
	if t.exporter != nil {
		return t.exporter.Shutdown()
	}
	return nil
}

// shouldSample determines if a trace should be sampled.
func (t *Tracer) shouldSample() bool {
	if t.sampleRate >= 1.0 {
		return true
	}
	if t.sampleRate <= 0 {
		return false
	}
	// Simple random sampling
	b := make([]byte, 1)
	rand.Read(b)
	return float64(b[0])/255.0 < t.sampleRate
}

// generateTraceID generates a new trace ID or uses existing from context.
func (t *Tracer) generateTraceID(ctx context.Context) string {
	if tc := t.TraceContextFromContext(ctx); tc != nil && tc.TraceID != "" {
		return tc.TraceID
	}
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// generateSpanID generates a new span ID.
func (t *Tracer) generateSpanID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Context key types
type spanContextKey struct{}
type traceContextKey struct{}

// ContextWithSpan returns a context with the span attached.
func (t *Tracer) ContextWithSpan(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, spanContextKey{}, span)
}

// SpanFromContext retrieves a span from context.
func (t *Tracer) SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(spanContextKey{}).(*Span); ok {
		return span
	}
	return nil
}

// ContextWithTraceContext returns a context with trace context attached.
func (t *Tracer) ContextWithTraceContext(ctx context.Context, tc *TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey{}, tc)
}

// TraceContextFromContext retrieves trace context from context.
func (t *Tracer) TraceContextFromContext(ctx context.Context) *TraceContext {
	if tc, ok := ctx.Value(traceContextKey{}).(*TraceContext); ok {
		return tc
	}
	return nil
}

// InjectTraceContext injects trace context into headers.
func (t *Tracer) InjectTraceContext(span *Span, headers map[string]string) {
	if span == nil {
		return
	}
	headers["traceparent"] = fmt.Sprintf("00-%s-%s-01", span.TraceID, span.SpanID)
}

// ExtractTraceContext extracts trace context from headers.
func (t *Tracer) ExtractTraceContext(headers map[string]string) *TraceContext {
	traceparent := headers["traceparent"]
	if traceparent == "" {
		return nil
	}

	// Parse W3C Trace Context format: version-traceid-spanid-flags
	var version, traceID, spanID, flags string
	_, err := fmt.Sscanf(traceparent, "%2s-%32s-%16s-%2s", &version, &traceID, &spanID, &flags)
	if err != nil {
		return nil
	}

	return &TraceContext{
		TraceID: traceID,
		SpanID:  spanID,
		Sampled: flags == "01",
	}
}
