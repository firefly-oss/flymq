package topic

import (
	"testing"
)

func TestMatcher(t *testing.T) {
	m := NewMatcher()
	m.Add("sensors/+/temperature")
	m.Add("sensors/#")
	m.Add("news/sports")

	tests := []struct {
		topic string
		match bool
	}{
		{"sensors/kitchen/temperature", true},
		{"sensors/livingroom/humidity", true},
		{"sensors/livingroom/temperature", true},
		{"sensors/temperature", true}, // sensors/# matches sensors/temperature
		{"news/sports", true},
		{"news/weather", false},
		{"other/topic", false},
	}

	for _, tt := range tests {
		if got := m.Match(tt.topic); got != tt.match {
			t.Errorf("Match(%q) = %v; want %v", tt.topic, got, tt.match)
		}
	}
}

func TestGetMatches(t *testing.T) {
	m := NewMatcher()
	m.Add("sensors/+/temperature")
	m.Add("sensors/#")
	m.Add("a/b/c")

	matches := m.GetMatches("sensors/kitchen/temperature")
	expected := []string{"sensors/+/temperature", "sensors/#"}

	if len(matches) != len(expected) {
		t.Errorf("expected %d matches, got %d", len(expected), len(matches))
	}

	for _, e := range expected {
		found := false
		for _, g := range matches {
			if e == g {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected match %q not found", e)
		}
	}
}

func TestIsPattern(t *testing.T) {
	if !IsPattern("a/+/b") {
		t.Error("expected a/+/b to be a pattern")
	}
	if !IsPattern("a/#") {
		t.Error("expected a/# to be a pattern")
	}
	if IsPattern("a/b/c") {
		t.Error("expected a/b/c not to be a pattern")
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		topic   string
		match   bool
	}{
		{"a/+/b", "a/c/b", true},
		{"a/+/b", "a/c/d", false},
		{"a/#", "a/b/c", true},
		{"a/#", "a", true},
		{"#", "any/topic/here", true},
		{"+/+/+", "a/b/c", true},
		{"+/+/+", "a/b", false},
	}

	for _, tt := range tests {
		if got := MatchPattern(tt.pattern, tt.topic); got != tt.match {
			t.Errorf("MatchPattern(%q, %q) = %v; want %v", tt.pattern, tt.topic, got, tt.match)
		}
	}
}
