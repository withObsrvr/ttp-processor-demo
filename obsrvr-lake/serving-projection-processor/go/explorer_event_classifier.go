package main

import (
	"context"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type explorerEventClassificationRule struct {
	RuleID         int
	Priority       int
	EventType      string
	Protocol       *string
	MatchContracts []string
	MatchTopic0    []string
	MatchTopicSig  *string
	topicSigRegex  *regexp.Regexp
}

type explorerEventClassification struct {
	EventType string
	Protocol  *string
}

type explorerEventClassifier struct {
	rules []explorerEventClassificationRule
}

func loadExplorerEventClassifier(ctx context.Context, pool *pgxpool.Pool) (*explorerEventClassifier, error) {
	rows, err := pool.Query(ctx, `
		SELECT rule_id, priority, event_type, protocol, match_contracts, match_topic0, match_topic_sig
		FROM event_classification_rules
		WHERE enabled = true
		ORDER BY priority DESC, rule_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []explorerEventClassificationRule
	for rows.Next() {
		var r explorerEventClassificationRule
		if err := rows.Scan(&r.RuleID, &r.Priority, &r.EventType, &r.Protocol, &r.MatchContracts, &r.MatchTopic0, &r.MatchTopicSig); err != nil {
			return nil, err
		}
		if r.MatchTopicSig != nil && *r.MatchTopicSig != "" {
			re, err := regexp.Compile(*r.MatchTopicSig)
			if err != nil {
				continue
			}
			r.topicSigRegex = re
		}
		rules = append(rules, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &explorerEventClassifier{rules: rules}, nil
}

func (c *explorerEventClassifier) Classify(contractID *string, topic0 *string, topicsDecoded *string) explorerEventClassification {
	for _, rule := range c.rules {
		if explorerRuleMatches(&rule, contractID, topic0, topicsDecoded) {
			return explorerEventClassification{EventType: rule.EventType, Protocol: rule.Protocol}
		}
	}
	return explorerEventClassification{EventType: "contract_call"}
}

func explorerRuleMatches(rule *explorerEventClassificationRule, contractID *string, topic0 *string, topicsDecoded *string) bool {
	if len(rule.MatchContracts) > 0 {
		if contractID == nil || !containsString(rule.MatchContracts, *contractID) {
			return false
		}
	}
	if len(rule.MatchTopic0) > 0 {
		if topic0 == nil || !containsString(rule.MatchTopic0, *topic0) {
			return false
		}
	}
	if rule.topicSigRegex != nil {
		if topicsDecoded == nil || !rule.topicSigRegex.MatchString(*topicsDecoded) {
			return false
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, v := range values {
		if strings.EqualFold(v, target) || v == target {
			return true
		}
	}
	return false
}
