package dolt

import (
	"net/url"
	"strings"
)

// Embedded Dolt DSN construction
//
// IMPORTANT: The embedded dolthub/driver treats everything after the "file://" prefix
// as a raw filesystem directory path (it does not URL-decode it). For compatibility,
// we intentionally do NOT URL-escape cfg.Path. We only URL-encode the query params.
//
// NOTE: This file intentionally only targets embedded mode ("dolt" database/sql driver).
// Server mode connections use the MySQL protocol driver and do not accept these DSN params.

func embeddedDefaultTuningParams() url.Values {
	// Reserved for embedded-only tuning params.
	// Note: low-level Dolt open behavior needed for retries (disable singleton cache,
	// fail fast on lock timeout) is enabled by the driver when open_retry=true.
	return url.Values{}
}

func embeddedDefaultRetryParams() url.Values {
	// These are embedded driver DSN params (see dolthub/driver):
	// - open_retry=true enables bounded retries during OpenConnector (engine open).
	// - open_retry_max_elapsed bounds total retry time.
	// - open_retry_max_tries is set high so open_retry_max_elapsed is the primary bound.
	return url.Values{
		"open_retry":             []string{"true"},
		"open_retry_max_elapsed": []string{"2s"},
		"open_retry_max_tries":   []string{"200"},
		"open_retry_initial":     []string{"10ms"},
		"open_retry_max_interval": []string{"100ms"},
	}
}

func embeddedDefaultOpenParams() url.Values {
	v := embeddedDefaultTuningParams()
	mergeURLValues(v, embeddedDefaultRetryParams())
	return v
}

func embeddedBaseParams(cfg *Config) url.Values {
	v := url.Values{}
	if cfg == nil {
		return v
	}
	if cfg.CommitterName != "" {
		v.Set("commitname", cfg.CommitterName)
	}
	if cfg.CommitterEmail != "" {
		v.Set("commitemail", cfg.CommitterEmail)
	}
	return v
}

func mergeURLValues(dst url.Values, src url.Values) {
	if dst == nil || src == nil {
		return
	}
	for k, vs := range src {
		// For DSN parameters we want deterministic single-value semantics.
		// Overwrite any existing values to avoid producing ambiguous query strings.
		dst[k] = append([]string(nil), vs...)
	}
}

func buildEmbeddedDSN(dir string, params url.Values) string {
	base := dir
	if !strings.HasPrefix(base, "file://") {
		base = "file://" + base
	}
	if len(params) == 0 {
		return base
	}
	return base + "?" + params.Encode()
}

// embeddedInitDSN builds the DSN used to create / open a multi-db directory without selecting a database.
func embeddedInitDSN(cfg *Config, extra url.Values) string {
	v := embeddedBaseParams(cfg)
	mergeURLValues(v, extra)
	return buildEmbeddedDSN(cfg.Path, v)
}

// embeddedDBDSN builds the DSN used for the main database connection (includes the database name).
func embeddedDBDSN(cfg *Config, extra url.Values) string {
	v := embeddedBaseParams(cfg)
	if cfg != nil && cfg.Database != "" {
		v.Set("database", cfg.Database)
	}
	mergeURLValues(v, extra)
	return buildEmbeddedDSN(cfg.Path, v)
}

