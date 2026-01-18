package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/logging"
	"flymq/internal/protocol"

	"github.com/gorilla/websocket"
)

// MockBroker is a simple mock for testing
type MockBroker struct {
	produceCalled   bool
	subscribeCalled bool
}

func (m *MockBroker) ProduceWithKeyAndPartition(topic string, key, data []byte) (uint64, int, error) {
	m.produceCalled = true
	return 1, 0, nil
}

func (m *MockBroker) ConsumeFromPartitionWithKey(topic string, partition int, offset uint64) (key []byte, value []byte, err error) {
	return nil, []byte("test"), nil
}

func (m *MockBroker) Subscribe(topic, groupID string, partition int, mode protocol.SubscribeMode) (uint64, error) {
	m.subscribeCalled = true
	return 0, nil
}

func (m *MockBroker) CommitOffset(topic, groupID string, partition int, offset uint64) (bool, error) {
	return true, nil
}

func (m *MockBroker) FetchWithKeys(topic string, partition int, offset uint64, maxMessages int, filter string) ([]broker.FetchedMessage, uint64, error) {
	return nil, offset, nil
}

func (m *MockBroker) ListTopics() []string {
	return []string{"test-topic"}
}

func (m *MockBroker) GetClusterMetadata(topic string) (*protocol.BinaryClusterMetadataResponse, error) {
	return &protocol.BinaryClusterMetadataResponse{}, nil
}

func TestGateway_HandleWebSocket(t *testing.T) {
	logger := logging.NewLogger("test")
	cfg := &config.Config{}
	mockBroker := &MockBroker{}
	gateway := NewGateway(cfg, mockBroker, nil, logger)

	server := httptest.NewServer(http.HandlerFunc(gateway.handleWebSocket))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Test Produce
	req := WSRequest{
		ID:      "1",
		Command: "produce",
		Params:  json.RawMessage(`{"topic":"test","value":"SGVsbG8="}`),
	}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("Failed to send produce: %v", err)
	}

	var resp WSResponse
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("Failed to read produce response: %v", err)
	}

	if !resp.Success {
		t.Errorf("Produce failed: %s", resp.Error)
	}
	if !mockBroker.produceCalled {
		t.Errorf("Broker Produce was not called")
	}

	// Test List Topics
	req = WSRequest{
		ID:      "2",
		Command: "list_topics",
	}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("Failed to send list_topics: %v", err)
	}

	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("Failed to read list_topics response: %v", err)
	}

	if !resp.Success {
		t.Errorf("List topics failed: %s", resp.Error)
	}

	// Test Subscribe
	req = WSRequest{
		ID:      "3",
		Command: "subscribe",
		Params:  json.RawMessage(`{"topic":"test","group_id":"test-group","partition":0,"mode":"earliest"}`),
	}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("Failed to send subscribe: %v", err)
	}

	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("Failed to read subscribe response: %v", err)
	}

	if !resp.Success {
		t.Errorf("Subscribe failed: %s", resp.Error)
	}
	if !mockBroker.subscribeCalled {
		t.Errorf("Broker Subscribe was not called")
	}

	// Wait a bit for subscription loop to run (though it won't push anything with our mock)
	time.Sleep(100 * time.Millisecond)
}
