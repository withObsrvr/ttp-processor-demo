package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/lib/pq"
)

// EventClassificationRule represents a single classification rule from the database
type EventClassificationRule struct {
	RuleID         int      `json:"rule_id"`
	Priority       int      `json:"priority"`
	EventType      string   `json:"event_type"`
	Protocol       *string  `json:"protocol,omitempty"`
	MatchContracts []string `json:"match_contracts,omitempty"`
	MatchTopic0    []string `json:"match_topic0,omitempty"`
	MatchTopicSig  *string  `json:"match_topic_sig,omitempty"`
	Description    *string  `json:"description,omitempty"`

	// compiled regex, not exported
	topicSigRegex *regexp.Regexp
}

// EventClassification is the result of classifying an event
type EventClassification struct {
	EventType string  `json:"type"`
	Protocol  *string `json:"protocol,omitempty"`
	RuleID    int     `json:"rule_id"`
}

// EventClassifier loads classification rules from the database and applies
// them to contract events in priority order. Rules are cached in memory
// and can be hot-reloaded without restarting the service.
type EventClassifier struct {
	mu    sync.RWMutex
	rules []EventClassificationRule
	db    *sql.DB // direct PostgreSQL connection (not DuckDB)
}

// NewEventClassifier creates a classifier and loads rules from PostgreSQL.
// Uses a direct PG connection (not DuckDB ATTACH) so pq.Array works reliably.
func NewEventClassifier(db *sql.DB) (*EventClassifier, error) {
	ec := &EventClassifier{db: db}
	if err := ec.LoadRules(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to load classification rules: %w", err)
	}
	return ec, nil
}

// LoadRules reads all enabled rules from event_classification_rules, ordered by priority DESC.
// This can be called at startup or to hot-reload rules.
func (ec *EventClassifier) LoadRules(ctx context.Context) error {
	query := `
		SELECT rule_id, priority, event_type, protocol,
		       match_contracts, match_topic0, match_topic_sig, description
		FROM event_classification_rules
		WHERE enabled = true
		ORDER BY priority DESC, rule_id ASC
	`

	rows, err := ec.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query classification rules: %w", err)
	}
	defer rows.Close()

	var rules []EventClassificationRule
	for rows.Next() {
		var r EventClassificationRule
		var matchSig sql.NullString

		if err := rows.Scan(&r.RuleID, &r.Priority, &r.EventType, &r.Protocol,
			pq.Array(&r.MatchContracts), pq.Array(&r.MatchTopic0),
			&matchSig, &r.Description); err != nil {
			return fmt.Errorf("scan rule: %w", err)
		}

		if matchSig.Valid && matchSig.String != "" {
			s := matchSig.String
			r.MatchTopicSig = &s
			compiled, err := regexp.Compile(s)
			if err != nil {
				log.Printf("WARNING: rule %d has invalid topic_sig regex %q: %v (skipping regex)", r.RuleID, s, err)
			} else {
				r.topicSigRegex = compiled
			}
		}

		rules = append(rules, r)
	}

	ec.mu.Lock()
	ec.rules = rules
	ec.mu.Unlock()

	log.Printf("EventClassifier: loaded %d classification rules", len(rules))
	return nil
}

// Classify determines the event type and protocol for a given event.
// Checks rules in priority order; first match wins.
func (ec *EventClassifier) Classify(contractID *string, topic0 *string, topicsDecoded *string) EventClassification {
	ec.mu.RLock()
	rules := ec.rules
	ec.mu.RUnlock()

	for _, rule := range rules {
		if ruleMatches(&rule, contractID, topic0, topicsDecoded) {
			return EventClassification{
				EventType: rule.EventType,
				Protocol:  rule.Protocol,
				RuleID:    rule.RuleID,
			}
		}
	}

	return EventClassification{EventType: "contract_call"}
}

// Rules returns the current loaded rules (for the admin/debug endpoint)
func (ec *EventClassifier) Rules() []EventClassificationRule {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	result := make([]EventClassificationRule, len(ec.rules))
	copy(result, ec.rules)
	return result
}

// KnownEventTypes returns the set of distinct event_type values from loaded rules
func (ec *EventClassifier) KnownEventTypes() map[string]bool {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	types := make(map[string]bool)
	for _, r := range ec.rules {
		types[r.EventType] = true
	}
	return types
}

// Topic0ValuesForTypes returns the set of topic0 values that map to the given event types,
// plus a flag indicating whether "contract_call" (the catch-all) is included.
// This is used to build SQL WHERE clauses for type filtering.
func (ec *EventClassifier) Topic0ValuesForTypes(types []string) (topic0Values []string, includesCatchAll bool) {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	typeSet := make(map[string]bool)
	for _, t := range types {
		typeSet[t] = true
	}

	// Collect all topic0 values from rules that match requested types
	seen := make(map[string]bool)
	for _, rule := range ec.rules {
		if typeSet[rule.EventType] {
			if len(rule.MatchTopic0) == 0 && len(rule.MatchContracts) == 0 {
				// This is a catch-all rule (no topic0 or contract filter)
				includesCatchAll = true
			}
			for _, t := range rule.MatchTopic0 {
				if !seen[t] {
					topic0Values = append(topic0Values, t)
					seen[t] = true
				}
			}
		}
	}

	return topic0Values, includesCatchAll
}

// AllKnownTopic0Values returns all topic0 values from non-catch-all rules.
// Used to build the NOT IN clause for "contract_call" filtering.
func (ec *EventClassifier) AllKnownTopic0Values() []string {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	seen := make(map[string]bool)
	var values []string
	for _, rule := range ec.rules {
		for _, t := range rule.MatchTopic0 {
			if !seen[t] {
				values = append(values, t)
				seen[t] = true
			}
		}
	}
	return values
}

func ruleMatches(rule *EventClassificationRule, contractID *string, topic0 *string, topicsDecoded *string) bool {
	// Check contract match (if specified)
	if len(rule.MatchContracts) > 0 {
		if contractID == nil {
			return false
		}
		if !stringInSlice(*contractID, rule.MatchContracts) {
			return false
		}
	}

	// Check topic0 match (if specified)
	if len(rule.MatchTopic0) > 0 {
		if topic0 == nil {
			return false
		}
		if !stringInSlice(*topic0, rule.MatchTopic0) {
			return false
		}
	}

	// Check topic signature regex (if specified)
	if rule.topicSigRegex != nil {
		if topicsDecoded == nil {
			return false
		}
		if !rule.topicSigRegex.MatchString(*topicsDecoded) {
			return false
		}
	}

	return true
}

func stringInSlice(s string, slice []string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
