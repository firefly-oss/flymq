package integration

import (
	"strings"
	"testing"

	"flymq/pkg/client"
)

func TestFailoverWhenSettingPublicAndAnonymousDisabled(t *testing.T) {
	// defaultPublic=false, allowAnonymous=false
	ts := newAuthTestServerWithOptions(t, false, false)

	// Connect as admin
	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer c.Close()

	// Try to make a topic public
	err = c.SetACL("test-topic", true, nil, nil)

	if err == nil {
		t.Errorf("Expected error when setting public topic with anonymous access disabled, but got nil")
	} else if !strings.Contains(err.Error(), "anonymous access is disabled") {
		t.Errorf("Expected error message to contain 'anonymous access is disabled', but got: %v", err)
	} else {
		t.Logf("Got expected error: %v", err)
	}
}
