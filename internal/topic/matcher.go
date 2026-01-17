package topic

import (
	"strings"
)

// Matcher provides efficient topic pattern matching.
// It supports MQTT-style wildcards:
// '+' matches exactly one topic level.
// '#' matches zero or more topic levels at the end of the pattern.
type Matcher struct {
	root *node
}

type node struct {
	children map[string]*node
	isEnd    bool
}

func newNode() *node {
	return &node{
		children: make(map[string]*node),
	}
}

// NewMatcher creates a new topic matcher.
func NewMatcher() *Matcher {
	return &Matcher{
		root: newNode(),
	}
}

// Add adds a topic pattern to the matcher.
func (m *Matcher) Add(pattern string) {
	levels := strings.Split(pattern, "/")
	current := m.root
	for _, level := range levels {
		if next, ok := current.children[level]; ok {
			current = next
		} else {
			next = newNode()
			current.children[level] = next
			current = next
		}
	}
	current.isEnd = true
}

// Match checks if a topic matches any pattern in the matcher.
func (m *Matcher) Match(topic string) bool {
	levels := strings.Split(topic, "/")
	return m.matchRecursive(m.root, levels)
}

func (m *Matcher) matchRecursive(n *node, levels []string) bool {
	if len(levels) == 0 {
		// If we've consumed all levels, check if this node is an end node
		// or if it has a '#' child which matches empty levels.
		if n.isEnd {
			return true
		}
		if hashNode, ok := n.children["#"]; ok && hashNode.isEnd {
			return true
		}
		return false
	}

	currentLevel := levels[0]

	// 1. Try exact match
	if next, ok := n.children[currentLevel]; ok {
		if m.matchRecursive(next, levels[1:]) {
			return true
		}
	}

	// 2. Try '+' wildcard
	if next, ok := n.children["+"]; ok {
		if m.matchRecursive(next, levels[1:]) {
			return true
		}
	}

	// 3. Try '#' wildcard
	if next, ok := n.children["#"]; ok {
		// '#' matches everything remaining, so it must be an end node
		if next.isEnd {
			return true
		}
	}

	return false
}

// GetMatches returns all patterns that match the given topic.
func (m *Matcher) GetMatches(topic string) []string {
	levels := strings.Split(topic, "/")
	var matches []string
	m.findMatchesRecursive(m.root, levels, "", &matches)
	return matches
}

func (m *Matcher) findMatchesRecursive(n *node, levels []string, currentPattern string, matches *[]string) {
	if len(levels) == 0 {
		if n.isEnd {
			*matches = append(*matches, strings.TrimPrefix(currentPattern, "/"))
		}
		if hashNode, ok := n.children["#"]; ok && hashNode.isEnd {
			*matches = append(*matches, strings.TrimPrefix(currentPattern+"/#", "/"))
		}
		return
	}

	currentLevel := levels[0]
	prefix := ""
	if currentPattern != "" {
		prefix = "/"
	}

	// Try exact match
	if next, ok := n.children[currentLevel]; ok {
		m.findMatchesRecursive(next, levels[1:], currentPattern+prefix+currentLevel, matches)
	}

	// Try '+' wildcard
	if next, ok := n.children["+"]; ok {
		m.findMatchesRecursive(next, levels[1:], currentPattern+prefix+"+", matches)
	}

	// Try '#' wildcard
	if next, ok := n.children["#"]; ok {
		if next.isEnd {
			*matches = append(*matches, strings.TrimPrefix(currentPattern+prefix+"#", "/"))
		}
	}
}

// IsPattern checks if the given string contains any wildcard characters.
func IsPattern(s string) bool {
	return strings.Contains(s, "+") || strings.Contains(s, "#")
}

// MatchPattern matches a topic against a single pattern.
func MatchPattern(pattern, topic string) bool {
	pLevels := strings.Split(pattern, "/")
	tLevels := strings.Split(topic, "/")

	return matchLevels(pLevels, tLevels)
}

func matchLevels(pLevels, tLevels []string) bool {
	if len(pLevels) == 0 && len(tLevels) == 0 {
		return true
	}
	if len(pLevels) == 0 {
		return false
	}

	if pLevels[0] == "#" {
		return true
	}

	if len(tLevels) == 0 {
		if pLevels[0] == "+" {
			return false
		}
		return false
	}

	if pLevels[0] == "+" || pLevels[0] == tLevels[0] {
		return matchLevels(pLevels[1:], tLevels[1:])
	}

	return false
}
