package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/stellar/go-stellar-sdk/support/render/hal"
	"github.com/stellar/go-stellar-sdk/support/render/problem"
)

const horizonCompatPrefix = "/api/v1/horizon-compat"

type horizonCompatLinkBuilder struct {
	origin *url.URL
	prefix string
}

func newHorizonCompatLinkBuilder(r *http.Request) horizonCompatLinkBuilder {
	rawBase := strings.TrimSpace(os.Getenv("HORIZON_COMPAT_BASE_URL"))
	if rawBase == "" {
		scheme := forwardedScheme(r)
		rawBase = fmt.Sprintf("%s://%s%s", scheme, r.Host, horizonCompatPrefix)
	}

	parsed, err := url.Parse(rawBase)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		parsed = &url.URL{Scheme: forwardedScheme(r), Host: r.Host, Path: horizonCompatPrefix}
	}

	origin := *parsed
	origin.Path = ""
	origin.RawPath = ""
	origin.RawQuery = ""
	origin.Fragment = ""

	prefix := strings.TrimRight(parsed.Path, "/")
	if prefix == "" {
		prefix = horizonCompatPrefix
	}

	return horizonCompatLinkBuilder{origin: &origin, prefix: prefix}
}

func forwardedScheme(r *http.Request) string {
	if proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		if idx := strings.Index(proto, ","); idx >= 0 {
			proto = proto[:idx]
		}
		proto = strings.TrimSpace(proto)
		if proto == "http" || proto == "https" {
			return proto
		}
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func (b horizonCompatLinkBuilder) Link(parts ...string) hal.Link {
	return hal.NewLink(b.href(joinHorizonPath(parts...)))
}

func (b horizonCompatLinkBuilder) PagedLink(parts ...string) hal.Link {
	link := b.Link(parts...)
	link.Href += hal.StandardPagingOptions
	link.PopulateTemplated()
	return link
}

func (b horizonCompatLinkBuilder) Linkf(format string, args ...interface{}) hal.Link {
	return hal.NewLink(b.href(fmt.Sprintf(format, args...)))
}

func (b horizonCompatLinkBuilder) href(relative string) string {
	relative = strings.TrimSpace(relative)
	if strings.HasPrefix(relative, "http://") || strings.HasPrefix(relative, "https://") {
		return relative
	}
	if !strings.HasPrefix(relative, "/") {
		relative = "/" + relative
	}
	base := strings.TrimRight(b.origin.String(), "/")
	return base + strings.TrimRight(b.prefix, "/") + relative
}

func joinHorizonPath(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			cleaned = append(cleaned, part)
		}
	}
	if len(cleaned) == 0 {
		return "/"
	}
	return "/" + strings.Join(cleaned, "/")
}

func horizonCompatCollectionLinks(r *http.Request, page horizonPageQuery, firstCursor, lastCursor string) hal.Links {
	return hal.Links{
		Self: horizonCompatRequestLink(r, page.Cursor, page.Order, page.Limit),
		Next: horizonCompatRequestLink(
			r,
			coalesceCursor(lastCursor, page.Cursor),
			page.Order,
			page.Limit,
		),
		Prev: horizonCompatRequestLink(
			r,
			coalesceCursor(firstCursor, page.Cursor),
			invertHorizonOrder(page.Order),
			page.Limit,
		),
	}
}

func horizonCompatRequestLink(r *http.Request, cursor, order string, limit uint64) hal.Link {
	relative := horizonCompatRelativeRequest(r)
	parsed, err := url.Parse(relative)
	if err != nil {
		parsed = &url.URL{Path: "/"}
	}
	q := parsed.Query()
	if cursor == "" {
		q.Del("cursor")
	} else {
		q.Set("cursor", cursor)
	}
	if order != "" {
		q.Set("order", order)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	parsed.RawQuery = q.Encode()
	return hal.NewLink(newHorizonCompatLinkBuilder(r).href(parsed.String()))
}

func horizonCompatRelativeRequest(r *http.Request) string {
	path := r.URL.Path
	if strings.HasPrefix(path, horizonCompatPrefix) {
		path = strings.TrimPrefix(path, horizonCompatPrefix)
	}
	if path == "" {
		path = "/"
	}
	if r.URL.RawQuery == "" {
		return path
	}
	return path + "?" + r.URL.RawQuery
}

func coalesceCursor(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

func invertHorizonOrder(order string) string {
	if order == "desc" {
		return "asc"
	}
	return "desc"
}

func writeHorizonJSON(w http.ResponseWriter, status int, data interface{}) error {
	js, err := json.Marshal(data)
	if err != nil {
		return err
	}
	js = append(js, '\n')
	w.Header().Set("Content-Type", "application/hal+json")
	w.WriteHeader(status)
	_, err = w.Write(js)
	return err
}

func renderHorizonProblem(w http.ResponseWriter, r *http.Request, p problem.P) {
	problem.Render(r.Context(), w, p)
}

func horizonProblem(status int, typ, title, detail string) problem.P {
	return problem.P{Type: typ, Title: title, Status: status, Detail: detail}
}
