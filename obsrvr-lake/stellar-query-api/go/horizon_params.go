package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	defaultHorizonLimit = uint64(10)
	maxHorizonLimit     = uint64(200)
)

type horizonPageQuery struct {
	Cursor string
	Order  string
	Limit  uint64
}

func parseHorizonPageQuery(r *http.Request) (horizonPageQuery, error) {
	q := r.URL.Query()
	out := horizonPageQuery{
		Cursor: q.Get("cursor"),
		Order:  strings.ToLower(strings.TrimSpace(q.Get("order"))),
		Limit:  defaultHorizonLimit,
	}
	if out.Order == "" {
		out.Order = "asc"
	}
	if out.Order != "asc" && out.Order != "desc" {
		return out, fmt.Errorf("order must be asc or desc")
	}
	if rawLimit := strings.TrimSpace(q.Get("limit")); rawLimit != "" {
		limit, err := strconv.ParseUint(rawLimit, 10, 64)
		if err != nil || limit == 0 {
			return out, fmt.Errorf("limit must be a positive integer")
		}
		if limit > maxHorizonLimit {
			limit = maxHorizonLimit
		}
		out.Limit = limit
	}
	return out, nil
}
