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

package metrics

import (
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	m := Get()
	if m == nil {
		t.Fatal("Expected non-nil metrics")
	}
}

func TestRecordProduce(t *testing.T) {
	m := &Metrics{}

	m.RecordProduce("test-topic", 100, 5*time.Millisecond)

	if m.MessagesProduced.Load() != 1 {
		t.Errorf("Expected MessagesProduced 1, got %d", m.MessagesProduced.Load())
	}

	tm := m.GetTopicMetrics("test-topic")
	if tm.MessagesProduced.Load() != 1 {
		t.Errorf("Expected topic MessagesProduced 1, got %d", tm.MessagesProduced.Load())
	}
	if tm.BytesProduced.Load() != 100 {
		t.Errorf("Expected topic BytesProduced 100, got %d", tm.BytesProduced.Load())
	}
}

func TestRecordConsume(t *testing.T) {
	m := &Metrics{}

	m.RecordConsume("test-topic", 200, 3*time.Millisecond)

	if m.MessagesConsumed.Load() != 1 {
		t.Errorf("Expected MessagesConsumed 1, got %d", m.MessagesConsumed.Load())
	}

	tm := m.GetTopicMetrics("test-topic")
	if tm.MessagesConsumed.Load() != 1 {
		t.Errorf("Expected topic MessagesConsumed 1, got %d", tm.MessagesConsumed.Load())
	}
	if tm.BytesConsumed.Load() != 200 {
		t.Errorf("Expected topic BytesConsumed 200, got %d", tm.BytesConsumed.Load())
	}
}

func TestRecordError(t *testing.T) {
	m := &Metrics{}

	m.RecordError()
	m.RecordError()

	if m.MessagesFailed.Load() != 2 {
		t.Errorf("Expected MessagesFailed 2, got %d", m.MessagesFailed.Load())
	}
}

func TestRecordExpired(t *testing.T) {
	m := &Metrics{}

	m.RecordExpired()

	if m.MessagesExpired.Load() != 1 {
		t.Errorf("Expected MessagesExpired 1, got %d", m.MessagesExpired.Load())
	}
}

func TestRecordDLQ(t *testing.T) {
	m := &Metrics{}

	m.RecordDLQ()

	if m.MessagesDLQ.Load() != 1 {
		t.Errorf("Expected MessagesDLQ 1, got %d", m.MessagesDLQ.Load())
	}
}

func TestConnectionMetrics(t *testing.T) {
	m := &Metrics{}

	m.ConnectionOpened()
	m.ConnectionOpened()
	m.ConnectionOpened()

	if m.ActiveConnections.Load() != 3 {
		t.Errorf("Expected ActiveConnections 3, got %d", m.ActiveConnections.Load())
	}
	if m.TotalConnections.Load() != 3 {
		t.Errorf("Expected TotalConnections 3, got %d", m.TotalConnections.Load())
	}

	m.ConnectionClosed()

	if m.ActiveConnections.Load() != 2 {
		t.Errorf("Expected ActiveConnections 2, got %d", m.ActiveConnections.Load())
	}
	if m.TotalConnections.Load() != 3 {
		t.Errorf("Expected TotalConnections still 3, got %d", m.TotalConnections.Load())
	}
}

func TestAverageProduceLatency(t *testing.T) {
	m := &Metrics{}

	// No data
	if m.AverageProduceLatency() != 0 {
		t.Error("Expected 0 latency with no data")
	}

	// Add some data
	m.RecordProduce("topic", 100, 1000*time.Microsecond)
	m.RecordProduce("topic", 100, 2000*time.Microsecond)
	m.RecordProduce("topic", 100, 3000*time.Microsecond)

	avg := m.AverageProduceLatency()
	if avg != 2000 {
		t.Errorf("Expected average latency 2000, got %f", avg)
	}
}

func TestAverageConsumeLatency(t *testing.T) {
	m := &Metrics{}

	// No data
	if m.AverageConsumeLatency() != 0 {
		t.Error("Expected 0 latency with no data")
	}

	// Add some data
	m.RecordConsume("topic", 100, 500*time.Microsecond)
	m.RecordConsume("topic", 100, 1500*time.Microsecond)

	avg := m.AverageConsumeLatency()
	if avg != 1000 {
		t.Errorf("Expected average latency 1000, got %f", avg)
	}
}

func TestGetTopicMetrics(t *testing.T) {
	m := &Metrics{}

	tm1 := m.GetTopicMetrics("topic-a")
	tm2 := m.GetTopicMetrics("topic-a")

	if tm1 != tm2 {
		t.Error("Expected same TopicMetrics instance for same topic")
	}

	tm3 := m.GetTopicMetrics("topic-b")
	if tm1 == tm3 {
		t.Error("Expected different TopicMetrics instance for different topic")
	}
}
