package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestValidatorIdentityProjectorLoadsAndNormalizesRadarNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/node" {
			t.Fatalf("unexpected Radar path %s", r.URL.Path)
		}
		fmt.Fprint(w, `[{
			"publicKey":" GVALIDATOR ",
			"name":" SDF Testnet 1 ",
			"alias":" sdf_testnet_1 ",
			"homeDomain":null,
			"organizationId":null,
			"dateUpdated":"2026-07-20T17:27:16.008Z",
			"isValidator":true,
			"ignoredVolatileField":42
		}]`)
	}))
	defer server.Close()

	projector := NewValidatorIdentityProjector("testnet", server.URL, time.Second, nil)
	nodes, err := projector.loadNodes(context.Background())
	if err != nil {
		t.Fatalf("loadNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if nodes[0].PublicKey != "GVALIDATOR" || nodes[0].Name != "SDF Testnet 1" || nodes[0].DisplayName != "SDF Testnet 1" {
		t.Fatalf("node was not normalized: %+v", nodes[0])
	}
	if !nodes[0].IsValidator {
		t.Fatalf("validator flag was lost: %+v", nodes[0])
	}
}

func TestValidatorIdentityFingerprintIgnoresVolatileRadarFields(t *testing.T) {
	before := normalizeRadarNodeIdentity(radarNodeIdentity{PublicKey: "GVALIDATOR", Name: "Node", DateUpdated: "2026-07-20T12:00:00Z", IsValidator: true})
	after := before
	after.DateUpdated = "2026-07-20T13:00:00Z"
	if validatorIdentityFingerprint(before) != validatorIdentityFingerprint(after) {
		t.Fatal("Radar observation timestamp incorrectly creates a new identity version")
	}
}
