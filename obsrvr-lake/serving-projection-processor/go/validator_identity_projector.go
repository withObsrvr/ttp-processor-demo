package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxRadarNodeResponseBytes = 8 << 20

type radarNodeIdentity struct {
	PublicKey      string `json:"publicKey"`
	Name           string `json:"name"`
	DisplayName    string `json:"displayName"`
	Alias          string `json:"alias"`
	HomeDomain     string `json:"homeDomain"`
	OrganizationID string `json:"organizationId"`
	DateUpdated    string `json:"dateUpdated"`
	IsValidator    bool   `json:"isValidator"`
}

type ValidatorIdentityProjector struct {
	network    string
	baseURL    string
	httpClient *http.Client
	targetPool *pgxpool.Pool
	now        func() time.Time
}

func NewValidatorIdentityProjector(network, baseURL string, timeout time.Duration, targetPool *pgxpool.Pool) *ValidatorIdentityProjector {
	return &ValidatorIdentityProjector{
		network:    network,
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: timeout},
		targetPool: targetPool,
		now:        time.Now,
	}
}

func (p *ValidatorIdentityProjector) Name() string { return "validator_identities" }

func (p *ValidatorIdentityProjector) RunOnce(ctx context.Context) (RunStats, error) {
	nodes, err := p.loadNodes(ctx)
	if err != nil {
		return RunStats{}, err
	}
	observedAt := p.now().UTC()
	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin validator identity tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, node := range nodes {
		node = normalizeRadarNodeIdentity(node)
		fingerprint := validatorIdentityFingerprint(node)
		var sourceUpdatedAt *time.Time
		if parsed, parseErr := time.Parse(time.RFC3339Nano, node.DateUpdated); parseErr == nil {
			parsed = parsed.UTC()
			sourceUpdatedAt = &parsed
		}

		if _, err := tx.Exec(ctx, `
			UPDATE serving.sv_validator_identity_history
			SET valid_to = $4
			WHERE network = $1 AND public_key = $2 AND valid_to IS NULL
			  AND identity_fingerprint <> $3
		`, p.network, node.PublicKey, fingerprint, observedAt); err != nil {
			return RunStats{}, fmt.Errorf("close validator identity history %s: %w", node.PublicKey, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_validator_identity_history (
				network, public_key, name, display_name, alias, home_domain,
				organization_id, is_validator, source, source_updated_at,
				identity_fingerprint, valid_from, valid_to
			)
			SELECT $1,$2,$3,$4,$5,$6,$7,$8,'radar',$9,$10,$11,NULL
			WHERE NOT EXISTS (
				SELECT 1 FROM serving.sv_validator_identity_history
				WHERE network = $1 AND public_key = $2 AND valid_to IS NULL
				  AND identity_fingerprint = $10
			)
		`, p.network, node.PublicKey, nullableText(node.Name), nullableText(node.DisplayName), nullableText(node.Alias),
			nullableText(node.HomeDomain), nullableText(node.OrganizationID), node.IsValidator, sourceUpdatedAt, fingerprint, observedAt); err != nil {
			return RunStats{}, fmt.Errorf("insert validator identity history %s: %w", node.PublicKey, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO serving.sv_validator_identity_current (
				network, public_key, name, display_name, alias, home_domain,
				organization_id, is_validator, source, source_updated_at,
				observed_at, identity_fingerprint
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'radar',$9,$10,$11)
			ON CONFLICT (network, public_key) DO UPDATE SET
				name = EXCLUDED.name,
				display_name = EXCLUDED.display_name,
				alias = EXCLUDED.alias,
				home_domain = EXCLUDED.home_domain,
				organization_id = EXCLUDED.organization_id,
				is_validator = EXCLUDED.is_validator,
				source = EXCLUDED.source,
				source_updated_at = EXCLUDED.source_updated_at,
				observed_at = EXCLUDED.observed_at,
				identity_fingerprint = EXCLUDED.identity_fingerprint
		`, p.network, node.PublicKey, nullableText(node.Name), nullableText(node.DisplayName), nullableText(node.Alias),
			nullableText(node.HomeDomain), nullableText(node.OrganizationID), node.IsValidator, sourceUpdatedAt, observedAt, fingerprint); err != nil {
			return RunStats{}, fmt.Errorf("upsert validator identity %s: %w", node.PublicKey, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit validator identities: %w", err)
	}
	return RunStats{RowsApplied: int64(len(nodes))}, nil
}

func (p *ValidatorIdentityProjector) loadNodes(ctx context.Context) ([]radarNodeIdentity, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/v1/node", nil)
	if err != nil {
		return nil, fmt.Errorf("build Radar nodes request: %w", err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch Radar nodes: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch Radar nodes: status %d", resp.StatusCode)
	}

	limited := io.LimitReader(resp.Body, maxRadarNodeResponseBytes+1)
	var nodes []radarNodeIdentity
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(&nodes); err != nil {
		return nil, fmt.Errorf("decode Radar nodes: %w", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("decode Radar nodes: trailing JSON")
	}
	seen := make(map[string]struct{}, len(nodes))
	for i := range nodes {
		nodes[i] = normalizeRadarNodeIdentity(nodes[i])
		if nodes[i].PublicKey == "" {
			return nil, fmt.Errorf("decode Radar nodes: node %d has no public key", i)
		}
		if _, exists := seen[nodes[i].PublicKey]; exists {
			return nil, fmt.Errorf("decode Radar nodes: duplicate public key %s", nodes[i].PublicKey)
		}
		seen[nodes[i].PublicKey] = struct{}{}
	}
	return nodes, nil
}

func normalizeRadarNodeIdentity(node radarNodeIdentity) radarNodeIdentity {
	node.PublicKey = strings.TrimSpace(node.PublicKey)
	node.Name = strings.TrimSpace(node.Name)
	node.DisplayName = strings.TrimSpace(node.DisplayName)
	node.Alias = strings.TrimSpace(node.Alias)
	node.HomeDomain = strings.TrimSpace(node.HomeDomain)
	node.OrganizationID = strings.TrimSpace(node.OrganizationID)
	node.DateUpdated = strings.TrimSpace(node.DateUpdated)
	if node.DisplayName == "" {
		switch {
		case node.Name != "":
			node.DisplayName = node.Name
		case node.Alias != "":
			node.DisplayName = node.Alias
		default:
			node.DisplayName = node.PublicKey
		}
	}
	return node
}

func validatorIdentityFingerprint(node radarNodeIdentity) string {
	hash := sha256.New()
	for _, field := range []string{node.PublicKey, node.Name, node.DisplayName, node.Alias, node.HomeDomain, node.OrganizationID, fmt.Sprint(node.IsValidator)} {
		_, _ = hash.Write([]byte(field))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func nullableText(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}
