package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
)

var smartAccountEventNames = []string{
	"context_rule_added",
	"context_rule_meta_updated",
	"context_rule_removed",
	"signer_added",
	"signer_removed",
	"signer_registered",
	"signer_deregistered",
	"policy_added",
	"policy_removed",
	"policy_registered",
	"policy_deregistered",
}

func smartAccountEventSQLList() string {
	quoted := make([]string, 0, len(smartAccountEventNames))
	for _, name := range smartAccountEventNames {
		quoted = append(quoted, "'"+name+"'")
	}
	return strings.Join(quoted, ", ")
}

type smartAccountEvent struct {
	eventID         string
	contractID      string
	ledgerSequence  int64
	transactionHash string
	closedAt        sql.NullTime
	eventName       string
	topic1          sql.NullString
	dataDecoded     sql.NullString
	operationIndex  sql.NullInt64
	eventIndex      sql.NullInt64
	transactionID   sql.NullInt64
	eventOrder      int64
}

type smartAccountSigner struct {
	signerID      sql.NullInt64
	signerType    sql.NullString
	signerAddress sql.NullString
	credentialID  sql.NullString
	rawBytes      sql.NullString
}

type smartAccountPolicy struct {
	policyID      sql.NullInt64
	policyAddress sql.NullString
	installParams sql.NullString
}

type smartAccountCache struct {
	signers  map[string]smartAccountSigner
	policies map[string]smartAccountPolicy
}

func newSmartAccountCache() *smartAccountCache {
	return &smartAccountCache{
		signers:  map[string]smartAccountSigner{},
		policies: map[string]smartAccountPolicy{},
	}
}

func smartAccountRegistryKey(contractID string, id int64) string {
	return contractID + ":" + strconv.FormatInt(id, 10)
}

func (rt *RealtimeTransformer) transformSmartAccountState(ctx context.Context, tx *sql.Tx, startLedger, endLedger int64) (int64, error) {
	rows, err := rt.sourceManager.QuerySmartAccountEvents(ctx, startLedger, endLedger)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	cache := newSmartAccountCache()
	count := int64(0)

	for rows.Next() {
		var ev smartAccountEvent
		if err := rows.Scan(
			&ev.eventID,
			&ev.contractID,
			&ev.ledgerSequence,
			&ev.transactionHash,
			&ev.closedAt,
			&ev.eventName,
			&ev.topic1,
			&ev.dataDecoded,
			&ev.operationIndex,
			&ev.eventIndex,
			&ev.transactionID,
		); err != nil {
			return count, fmt.Errorf("scan smart account event: %w", err)
		}

		contractID, err := hexToStrKey(ev.contractID)
		if err != nil {
			log.Printf("⚠️  smart account event %s has invalid contract_id %q: %v", ev.eventID, ev.contractID, err)
			continue
		}
		ev.contractID = contractID
		ev.eventOrder = smartAccountEventOrder(ev)

		entries, err := smartAccountDataEntries(ev.dataDecoded)
		if err != nil {
			log.Printf("⚠️  smart account event %s data parse failed: %v", ev.eventID, err)
			continue
		}

		if err := rt.applySmartAccountEvent(ctx, tx, ev, entries, cache); err != nil {
			return count, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("iterate smart account events: %w", err)
	}

	return count, nil
}

func smartAccountEventOrder(ev smartAccountEvent) int64 {
	op := int64(0)
	if ev.operationIndex.Valid {
		op = ev.operationIndex.Int64 + 1
	}
	if ev.transactionID.Valid && ev.transactionID.Int64 > 0 {
		return ev.transactionID.Int64 | (op & 4095)
	}
	return (ev.ledgerSequence << 32) | (op & 4095)
}

func smartAccountDataEntries(data sql.NullString) (map[string]interface{}, error) {
	if !data.Valid || strings.TrimSpace(data.String) == "" {
		return map[string]interface{}{}, nil
	}
	dec := json.NewDecoder(bytes.NewBufferString(data.String))
	dec.UseNumber()
	var root map[string]interface{}
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	if entries, ok := root["entries"].(map[string]interface{}); ok {
		return entries, nil
	}
	return root, nil
}

func (rt *RealtimeTransformer) applySmartAccountEvent(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, entries map[string]interface{}, cache *smartAccountCache) error {
	ruleID, hasRuleID := smartAccountTopicID(ev.topic1)

	switch ev.eventName {
	case "context_rule_added":
		if !hasRuleID {
			return nil
		}
		if err := upsertSmartAccountRule(ctx, tx, ev, ruleID, true, nil); err != nil {
			return err
		}
		for _, signer := range parseSmartAccountSignerList(entries["signers"]) {
			if err := upsertSmartAccountRuleSigner(ctx, tx, ev, ruleID, signer, true); err != nil {
				return err
			}
		}
		for _, signerID := range smartAccountIDList(entries["signer_ids"]) {
			signer := smartAccountSigner{signerID: sql.NullInt64{Int64: signerID, Valid: true}}
			if resolved, ok := cache.signers[smartAccountRegistryKey(ev.contractID, signerID)]; ok {
				signer = mergeSmartAccountSigner(signer, resolved)
			} else if resolved, ok, err := loadRegisteredSmartAccountSigner(ctx, tx, ev.contractID, signerID); err != nil {
				return err
			} else if ok {
				signer = mergeSmartAccountSigner(signer, resolved)
			}
			if err := upsertSmartAccountRuleSigner(ctx, tx, ev, ruleID, signer, true); err != nil {
				return err
			}
		}
		for _, policy := range parseSmartAccountPolicyList(entries["policies"]) {
			if err := upsertSmartAccountRulePolicy(ctx, tx, ev, ruleID, policy, true); err != nil {
				return err
			}
		}
		for _, policyID := range smartAccountIDList(entries["policy_ids"]) {
			policy := smartAccountPolicy{policyID: sql.NullInt64{Int64: policyID, Valid: true}}
			if resolved, ok := cache.policies[smartAccountRegistryKey(ev.contractID, policyID)]; ok {
				policy = mergeSmartAccountPolicy(policy, resolved)
			} else if resolved, ok, err := loadRegisteredSmartAccountPolicy(ctx, tx, ev.contractID, policyID); err != nil {
				return err
			} else if ok {
				policy = mergeSmartAccountPolicy(policy, resolved)
			}
			if err := upsertSmartAccountRulePolicy(ctx, tx, ev, ruleID, policy, true); err != nil {
				return err
			}
		}
	case "context_rule_meta_updated":
		if !hasRuleID {
			return nil
		}
		meta, _ := json.Marshal(entries)
		if err := upsertSmartAccountRule(ctx, tx, ev, ruleID, true, stringPtr(string(meta))); err != nil {
			return err
		}
	case "context_rule_removed":
		if !hasRuleID {
			return nil
		}
		if err := upsertSmartAccountRule(ctx, tx, ev, ruleID, false, nil); err != nil {
			return err
		}
	case "signer_registered":
		signerID, ok := smartAccountTopicID(ev.topic1)
		if !ok {
			return nil
		}
		signer, ok := parseSmartAccountSigner(entries["signer"])
		if !ok {
			signer = smartAccountSigner{}
		}
		signer.signerID = sql.NullInt64{Int64: signerID, Valid: true}
		cache.signers[smartAccountRegistryKey(ev.contractID, signerID)] = signer
		if err := upsertSmartAccountRegistrySigner(ctx, tx, ev, signer, true); err != nil {
			return err
		}
	case "signer_deregistered":
		signerID, ok := smartAccountTopicID(ev.topic1)
		if !ok {
			return nil
		}
		signer := smartAccountSigner{signerID: sql.NullInt64{Int64: signerID, Valid: true}}
		if resolved, ok := cache.signers[smartAccountRegistryKey(ev.contractID, signerID)]; ok {
			signer = mergeSmartAccountSigner(signer, resolved)
		}
		if err := upsertSmartAccountRegistrySigner(ctx, tx, ev, signer, false); err != nil {
			return err
		}
	case "signer_added", "signer_removed":
		if !hasRuleID {
			return nil
		}
		active := ev.eventName == "signer_added"
		if signerID, ok := smartAccountEntryID(entries["signer_id"]); ok {
			signer := smartAccountSigner{signerID: sql.NullInt64{Int64: signerID, Valid: true}}
			if resolved, ok := cache.signers[smartAccountRegistryKey(ev.contractID, signerID)]; ok {
				signer = mergeSmartAccountSigner(signer, resolved)
			} else if resolved, ok, err := loadRegisteredSmartAccountSigner(ctx, tx, ev.contractID, signerID); err != nil {
				return err
			} else if ok {
				signer = mergeSmartAccountSigner(signer, resolved)
			}
			return upsertSmartAccountRuleSigner(ctx, tx, ev, ruleID, signer, active)
		}
		if signer, ok := parseSmartAccountSigner(entries["signer"]); ok {
			return upsertSmartAccountRuleSigner(ctx, tx, ev, ruleID, signer, active)
		}
	case "policy_registered":
		policyID, ok := smartAccountTopicID(ev.topic1)
		if !ok {
			return nil
		}
		policy := parseSmartAccountPolicy(entries["policy"])
		policy.policyID = sql.NullInt64{Int64: policyID, Valid: true}
		cache.policies[smartAccountRegistryKey(ev.contractID, policyID)] = policy
		if err := upsertSmartAccountRegistryPolicy(ctx, tx, ev, policy, true); err != nil {
			return err
		}
	case "policy_deregistered":
		policyID, ok := smartAccountTopicID(ev.topic1)
		if !ok {
			return nil
		}
		policy := smartAccountPolicy{policyID: sql.NullInt64{Int64: policyID, Valid: true}}
		if resolved, ok := cache.policies[smartAccountRegistryKey(ev.contractID, policyID)]; ok {
			policy = mergeSmartAccountPolicy(policy, resolved)
		}
		if err := upsertSmartAccountRegistryPolicy(ctx, tx, ev, policy, false); err != nil {
			return err
		}
	case "policy_added", "policy_removed":
		if !hasRuleID {
			return nil
		}
		active := ev.eventName == "policy_added"
		policyID, ok := smartAccountEntryID(entries["policy_id"])
		if !ok {
			return nil
		}
		policy := smartAccountPolicy{policyID: sql.NullInt64{Int64: policyID, Valid: true}}
		if resolved, ok := cache.policies[smartAccountRegistryKey(ev.contractID, policyID)]; ok {
			policy = mergeSmartAccountPolicy(policy, resolved)
		} else if resolved, ok, err := loadRegisteredSmartAccountPolicy(ctx, tx, ev.contractID, policyID); err != nil {
			return err
		} else if ok {
			policy = mergeSmartAccountPolicy(policy, resolved)
		}
		if install, ok := marshalNullableJSON(entries["install_param"]); ok {
			policy.installParams = sql.NullString{String: install, Valid: true}
		}
		return upsertSmartAccountRulePolicy(ctx, tx, ev, ruleID, policy, active)
	}

	return nil
}

func upsertSmartAccountRule(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, ruleID int64, active bool, metadata *string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO smart_account_context_rules (
			contract_id, context_rule_id, active, metadata, event_type, ledger_sequence,
			last_modified_ledger, transaction_hash, operation_index, event_index,
			event_order, closed_at, updated_at
		) VALUES ($1,$2,$3,CAST($4 AS jsonb),$5,$6,$6,$7,$8,$9,$10,$11,NOW())
		ON CONFLICT (contract_id, context_rule_id) DO UPDATE SET
			active = EXCLUDED.active,
			metadata = COALESCE(EXCLUDED.metadata, smart_account_context_rules.metadata),
			event_type = EXCLUDED.event_type,
			ledger_sequence = EXCLUDED.ledger_sequence,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			transaction_hash = EXCLUDED.transaction_hash,
			operation_index = EXCLUDED.operation_index,
			event_index = EXCLUDED.event_index,
			event_order = EXCLUDED.event_order,
			closed_at = EXCLUDED.closed_at,
			updated_at = NOW()
		WHERE (EXCLUDED.event_order, COALESCE(EXCLUDED.event_index, -1)) >= (smart_account_context_rules.event_order, COALESCE(smart_account_context_rules.event_index, -1))
	`, ev.contractID, ruleID, active, metadata, ev.eventName, ev.ledgerSequence, ev.transactionHash, nullableInt(ev.operationIndex), nullableInt(ev.eventIndex), ev.eventOrder, nullableTime(ev.closedAt))
	return err
}

func upsertSmartAccountRegistrySigner(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, signer smartAccountSigner, active bool) error {
	return upsertSmartAccountSigner(ctx, tx, ev, "registry", -1, signer, active)
}

func upsertSmartAccountRuleSigner(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, ruleID int64, signer smartAccountSigner, active bool) error {
	return upsertSmartAccountSigner(ctx, tx, ev, "rule", ruleID, signer, active)
}

func upsertSmartAccountSigner(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, scope string, ruleID int64, signer smartAccountSigner, active bool) error {
	key := smartAccountSignerKey(signer)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO smart_account_signers (
			contract_id, scope, context_rule_id, signer_key, signer_id, signer_type,
			signer_address, credential_id, raw_bytes, active, event_type, ledger_sequence,
			last_modified_ledger, transaction_hash, operation_index, event_index,
			event_order, closed_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12,$13,$14,$15,$16,$17,NOW())
		ON CONFLICT (contract_id, scope, context_rule_id, signer_key) DO UPDATE SET
			signer_id = COALESCE(EXCLUDED.signer_id, smart_account_signers.signer_id),
			signer_type = COALESCE(EXCLUDED.signer_type, smart_account_signers.signer_type),
			signer_address = COALESCE(EXCLUDED.signer_address, smart_account_signers.signer_address),
			credential_id = COALESCE(EXCLUDED.credential_id, smart_account_signers.credential_id),
			raw_bytes = COALESCE(EXCLUDED.raw_bytes, smart_account_signers.raw_bytes),
			active = EXCLUDED.active,
			event_type = EXCLUDED.event_type,
			ledger_sequence = EXCLUDED.ledger_sequence,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			transaction_hash = EXCLUDED.transaction_hash,
			operation_index = EXCLUDED.operation_index,
			event_index = EXCLUDED.event_index,
			event_order = EXCLUDED.event_order,
			closed_at = EXCLUDED.closed_at,
			updated_at = NOW()
		WHERE (EXCLUDED.event_order, COALESCE(EXCLUDED.event_index, -1)) >= (smart_account_signers.event_order, COALESCE(smart_account_signers.event_index, -1))
	`, ev.contractID, scope, ruleID, key, nullableInt64(signer.signerID), nullableString(signer.signerType), nullableString(signer.signerAddress), nullableString(signer.credentialID), nullableString(signer.rawBytes), active, ev.eventName, ev.ledgerSequence, ev.transactionHash, nullableInt(ev.operationIndex), nullableInt(ev.eventIndex), ev.eventOrder, nullableTime(ev.closedAt))
	return err
}

func upsertSmartAccountRegistryPolicy(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, policy smartAccountPolicy, active bool) error {
	return upsertSmartAccountPolicy(ctx, tx, ev, "registry", -1, policy, active)
}

func upsertSmartAccountRulePolicy(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, ruleID int64, policy smartAccountPolicy, active bool) error {
	return upsertSmartAccountPolicy(ctx, tx, ev, "rule", ruleID, policy, active)
}

func upsertSmartAccountPolicy(ctx context.Context, tx *sql.Tx, ev smartAccountEvent, scope string, ruleID int64, policy smartAccountPolicy, active bool) error {
	key := smartAccountPolicyKey(policy)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO smart_account_policies (
			contract_id, scope, context_rule_id, policy_key, policy_id, policy_address,
			install_params, active, event_type, ledger_sequence, last_modified_ledger,
			transaction_hash, operation_index, event_index, event_order, closed_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS jsonb),$8,$9,$10,$10,$11,$12,$13,$14,$15,NOW())
		ON CONFLICT (contract_id, scope, context_rule_id, policy_key) DO UPDATE SET
			policy_id = COALESCE(EXCLUDED.policy_id, smart_account_policies.policy_id),
			policy_address = COALESCE(EXCLUDED.policy_address, smart_account_policies.policy_address),
			install_params = COALESCE(EXCLUDED.install_params, smart_account_policies.install_params),
			active = EXCLUDED.active,
			event_type = EXCLUDED.event_type,
			ledger_sequence = EXCLUDED.ledger_sequence,
			last_modified_ledger = EXCLUDED.last_modified_ledger,
			transaction_hash = EXCLUDED.transaction_hash,
			operation_index = EXCLUDED.operation_index,
			event_index = EXCLUDED.event_index,
			event_order = EXCLUDED.event_order,
			closed_at = EXCLUDED.closed_at,
			updated_at = NOW()
		WHERE (EXCLUDED.event_order, COALESCE(EXCLUDED.event_index, -1)) >= (smart_account_policies.event_order, COALESCE(smart_account_policies.event_index, -1))
	`, ev.contractID, scope, ruleID, key, nullableInt64(policy.policyID), nullableString(policy.policyAddress), nullableString(policy.installParams), active, ev.eventName, ev.ledgerSequence, ev.transactionHash, nullableInt(ev.operationIndex), nullableInt(ev.eventIndex), ev.eventOrder, nullableTime(ev.closedAt))
	return err
}

func loadRegisteredSmartAccountSigner(ctx context.Context, tx *sql.Tx, contractID string, signerID int64) (smartAccountSigner, bool, error) {
	var signer smartAccountSigner
	err := tx.QueryRowContext(ctx, `
		SELECT signer_id, signer_type, signer_address, credential_id, raw_bytes
		FROM smart_account_signers
		WHERE contract_id = $1 AND scope = 'registry' AND signer_id = $2 AND active = true
	`, contractID, signerID).Scan(&signer.signerID, &signer.signerType, &signer.signerAddress, &signer.credentialID, &signer.rawBytes)
	if err == sql.ErrNoRows {
		return signer, false, nil
	}
	if err != nil {
		return signer, false, err
	}
	return signer, true, nil
}

func loadRegisteredSmartAccountPolicy(ctx context.Context, tx *sql.Tx, contractID string, policyID int64) (smartAccountPolicy, bool, error) {
	var policy smartAccountPolicy
	err := tx.QueryRowContext(ctx, `
		SELECT policy_id, policy_address, install_params::text
		FROM smart_account_policies
		WHERE contract_id = $1 AND scope = 'registry' AND policy_id = $2 AND active = true
	`, contractID, policyID).Scan(&policy.policyID, &policy.policyAddress, &policy.installParams)
	if err == sql.ErrNoRows {
		return policy, false, nil
	}
	if err != nil {
		return policy, false, err
	}
	return policy, true, nil
}

func parseSmartAccountSignerList(v interface{}) []smartAccountSigner {
	items := smartAccountArray(v)
	if len(items) == 0 {
		return nil
	}
	out := make([]smartAccountSigner, 0, len(items))
	for _, item := range items {
		if signer, ok := parseSmartAccountSigner(item); ok {
			out = append(out, signer)
		}
	}
	return out
}

func parseSmartAccountSigner(v interface{}) (smartAccountSigner, bool) {
	vec := smartAccountArray(v)
	if len(vec) < 2 {
		return smartAccountSigner{}, false
	}
	signer := smartAccountSigner{
		signerType:    nullStringFromString(asSmartAccountString(vec[0])),
		signerAddress: nullStringFromString(smartAccountAddress(vec[1])),
	}
	rawBytes := ""
	if len(vec) >= 3 {
		rawBytes = smartAccountBytesHex(vec[2])
		signer.rawBytes = nullStringFromString(rawBytes)
	}
	if rawBytes != "" {
		credential := ""
		if strings.EqualFold(signer.signerType.String, "External") && len(rawBytes) > 130 {
			credential = strings.ToLower(rawBytes[130:])
		}
		if credential != "" {
			signer.credentialID = sql.NullString{String: credential, Valid: true}
		}
	}
	return signer, true
}

func parseSmartAccountPolicyList(v interface{}) []smartAccountPolicy {
	items := smartAccountArray(v)
	if len(items) == 0 {
		return nil
	}
	out := make([]smartAccountPolicy, 0, len(items))
	for _, item := range items {
		policy := parseSmartAccountPolicy(item)
		if policy.policyAddress.Valid {
			out = append(out, policy)
		}
	}
	return out
}

func parseSmartAccountPolicy(v interface{}) smartAccountPolicy {
	return smartAccountPolicy{policyAddress: nullStringFromString(smartAccountAddress(v))}
}

func smartAccountIDList(v interface{}) []int64 {
	items := smartAccountArray(v)
	if len(items) == 0 {
		return nil
	}
	out := make([]int64, 0, len(items))
	for _, item := range items {
		if id, ok := smartAccountEntryID(item); ok {
			out = append(out, id)
		}
	}
	return out
}

func smartAccountArray(v interface{}) []interface{} {
	switch t := v.(type) {
	case []interface{}:
		return t
	case map[string]interface{}:
		for _, key := range []string{"values", "items"} {
			if items, ok := t[key].([]interface{}); ok {
				return items
			}
		}
	}
	return nil
}

func smartAccountTopicID(topic sql.NullString) (int64, bool) {
	if !topic.Valid {
		return 0, false
	}
	raw := strings.TrimSpace(topic.String)
	if strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[") {
		dec := json.NewDecoder(bytes.NewBufferString(raw))
		dec.UseNumber()
		var decoded interface{}
		if err := dec.Decode(&decoded); err == nil {
			return smartAccountEntryID(decoded)
		}
	}
	return smartAccountEntryID(raw)
}

func smartAccountEntryID(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case nil:
		return 0, false
	case json.Number:
		i, err := t.Int64()
		return i, err == nil
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return i, err == nil
	case map[string]interface{}:
		for _, key := range []string{"u32", "value"} {
			if val, ok := t[key]; ok {
				return smartAccountEntryID(val)
			}
		}
	}
	return 0, false
}

func smartAccountAddress(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]interface{}:
		if addr, ok := t["address"].(string); ok {
			return addr
		}
	}
	return ""
}

func smartAccountBytesHex(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.ToLower(strings.TrimPrefix(t, "0x"))
	case map[string]interface{}:
		if hexValue, ok := t["hex"].(string); ok {
			return strings.ToLower(strings.TrimPrefix(hexValue, "0x"))
		}
	}
	return ""
}

func asSmartAccountString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]interface{}:
		for _, key := range []string{"symbol", "value", "type"} {
			if val, ok := t[key].(string); ok {
				return val
			}
		}
	}
	return ""
}

func smartAccountSignerKey(signer smartAccountSigner) string {
	if signer.signerID.Valid {
		return fmt.Sprintf("id:%d", signer.signerID.Int64)
	}
	parts := []string{signer.signerType.String, signer.signerAddress.String, signer.credentialID.String}
	return strings.Join(parts, "|")
}

func smartAccountPolicyKey(policy smartAccountPolicy) string {
	if policy.policyID.Valid {
		return fmt.Sprintf("id:%d", policy.policyID.Int64)
	}
	if policy.policyAddress.Valid {
		return policy.policyAddress.String
	}
	return "unknown"
}

func mergeSmartAccountSigner(base, overlay smartAccountSigner) smartAccountSigner {
	if !base.signerID.Valid {
		base.signerID = overlay.signerID
	}
	if !base.signerType.Valid {
		base.signerType = overlay.signerType
	}
	if !base.signerAddress.Valid {
		base.signerAddress = overlay.signerAddress
	}
	if !base.credentialID.Valid {
		base.credentialID = overlay.credentialID
	}
	if !base.rawBytes.Valid {
		base.rawBytes = overlay.rawBytes
	}
	return base
}

func mergeSmartAccountPolicy(base, overlay smartAccountPolicy) smartAccountPolicy {
	if !base.policyID.Valid {
		base.policyID = overlay.policyID
	}
	if !base.policyAddress.Valid {
		base.policyAddress = overlay.policyAddress
	}
	if !base.installParams.Valid {
		base.installParams = overlay.installParams
	}
	return base
}

func marshalNullableJSON(v interface{}) (string, bool) {
	if v == nil {
		return "", false
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return string(b), true
}

func nullableString(v sql.NullString) interface{} {
	if !v.Valid {
		return nil
	}
	return v.String
}

func nullableInt64(v sql.NullInt64) interface{} {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func nullableInt(v sql.NullInt64) interface{} {
	if !v.Valid {
		return nil
	}
	return int(v.Int64)
}

func nullableTime(v sql.NullTime) interface{} {
	if !v.Valid {
		return nil
	}
	return v.Time
}

func nullStringFromString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func stringPtr(s string) *string {
	return &s
}
