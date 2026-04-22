package tokensymbol

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Resolver loads ERC-20 / NEP-17 token symbol via chain RPC (cached per process).
type Resolver struct {
	http          *http.Client
	n3Endpoints   []string
	neoxEndpoints []string
	cache         map[string]string
}

// New returns a symbol resolver, or use nil at call sites to skip resolution.
func New(n3Endpoints, neoxEndpoints []string) *Resolver {
	return &Resolver{
		http:          &http.Client{Timeout: 6 * time.Second},
		n3Endpoints:   n3Endpoints,
		neoxEndpoints: neoxEndpoints,
		cache:         make(map[string]string),
	}
}

// ResolveN3Symbol fetches the symbol for an N3 contract (neo_n3) via invokefunction.
func (r *Resolver) ResolveN3Symbol(ctx context.Context, contract string) string {
	if r == nil || contract == "" || contract == "unknown" {
		return ""
	}
	cacheKey := "n3|" + strings.ToLower(contract)
	if v, ok := r.cache[cacheKey]; ok {
		return v
	}
	for _, ep := range r.n3Endpoints {
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			continue
		}
		body := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "invokefunction",
			"params":  []interface{}{contract, "symbol"},
		}
		raw, err := r.rpcCall(ctx, ep, body)
		if err != nil {
			continue
		}
		s := parseN3Symbol(raw)
		if s != "" {
			r.cache[cacheKey] = s
			return s
		}
	}
	r.cache[cacheKey] = ""
	return ""
}

// ResolveNeoXSymbol fetches the symbol for a NeoX contract via eth_call symbol().
func (r *Resolver) ResolveNeoXSymbol(ctx context.Context, contract string) string {
	if r == nil || contract == "" || contract == "unknown" {
		return ""
	}
	cacheKey := "neox|" + strings.ToLower(contract)
	if v, ok := r.cache[cacheKey]; ok {
		return v
	}
	for _, ep := range r.neoxEndpoints {
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			continue
		}
		body := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "eth_call",
			"params": []interface{}{
				map[string]string{
					"to":   contract,
					"data": "0x95d89b41", // symbol()
				},
				"latest",
			},
		}
		raw, err := r.rpcCall(ctx, ep, body)
		if err != nil {
			continue
		}
		s := parseEthCallSymbol(raw)
		if s != "" {
			r.cache[cacheKey] = s
			return s
		}
	}
	r.cache[cacheKey] = ""
	return ""
}

// ResolveOnChain returns the symbol for a token on the given source or destination chain key.
func (r *Resolver) ResolveOnChain(ctx context.Context, chain, contract string) string {
	if r == nil {
		return ""
	}
	switch chain {
	case "neo_n3":
		return r.ResolveN3Symbol(ctx, contract)
	case "neo_x":
		return r.ResolveNeoXSymbol(ctx, contract)
	default:
		return ""
	}
}

func (r *Resolver) rpcCall(ctx context.Context, endpoint string, body map[string]interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(b)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("http %s", res.Status)
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		return nil, err
	}
	if raw["error"] != nil {
		return nil, fmt.Errorf("rpc error")
	}
	return raw, nil
}

func parseN3Symbol(raw map[string]interface{}) string {
	result, ok := raw["result"].(map[string]interface{})
	if !ok {
		return ""
	}
	stack, ok := result["stack"].([]interface{})
	if !ok || len(stack) == 0 {
		return ""
	}
	item, ok := stack[0].(map[string]interface{})
	if !ok {
		return ""
	}
	typ, _ := item["type"].(string)
	val, _ := item["value"].(string)
	if val == "" {
		return ""
	}
	switch typ {
	case "String":
		return strings.TrimSpace(strings.TrimRight(val, "\x00"))
	case "ByteString", "Buffer", "ByteArray":
		if b, err := base64.StdEncoding.DecodeString(val); err == nil {
			return strings.TrimSpace(strings.TrimRight(string(b), "\x00"))
		}
		if b, err := hex.DecodeString(strings.TrimPrefix(val, "0x")); err == nil {
			return strings.TrimSpace(strings.TrimRight(string(b), "\x00"))
		}
	}
	return ""
}

func parseEthCallSymbol(raw map[string]interface{}) string {
	hexOut, ok := raw["result"].(string)
	if !ok || hexOut == "" || hexOut == "0x" {
		return ""
	}
	out, err := hex.DecodeString(strings.TrimPrefix(hexOut, "0x"))
	if err != nil || len(out) == 0 {
		return ""
	}

	if len(out) == 32 {
		return strings.TrimSpace(strings.TrimRight(string(out), "\x00"))
	}

	if len(out) < 96 {
		return ""
	}
	dataLen := int(out[63])
	if dataLen <= 0 {
		return ""
	}
	start := 64
	end := start + dataLen
	if end > len(out) {
		return ""
	}
	return strings.TrimSpace(strings.TrimRight(string(out[start:end]), "\x00"))
}
