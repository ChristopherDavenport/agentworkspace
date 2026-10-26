package remote

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// send sends one request as a browser, or a program, might: headers as
// given, and Host overriding the URL's when not empty.
func send(t *testing.T, method, url, host, token, body string, header map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		req.Host = host
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	code := ""
	if resp.StatusCode/100 != 2 {
		var r errorReply
		if err := decodeReply(resp, &r); err == nil {
			code = r.Error.Code
		}
	}
	return resp.StatusCode, code
}

// A page the user opens cannot run a command through an unauthenticated
// handler on loopback: the review's request, a CORS simple request
// with another site's Origin and a rebound Host, is refused, and runs
// nothing.
func TestABrowsersSimpleRequestRunsNothing(t *testing.T) {
	l, dir := newLocal(t, pathEnv())
	_, url := serve(t, l, WithInsecureNoAuth())
	status, _ := send(t, "POST", url+"/v1/exec", "evil.example:1234", "", `{"args":["sh","-c","echo pwned > pwned"]}`, map[string]string{
		"Content-Type": "text/plain",
		"Origin":       "http://evil.example",
	})
	if status/100 == 2 {
		t.Errorf("the request was served: %d", status)
	}
	if _, err := os.Stat(filepath.Join(dir, "pwned")); err == nil {
		t.Error("the page's command ran")
	}
}

// Each check alone: an Origin, a cross-site Sec-Fetch-Site and a body
// of another type are refused in every mode, and a Host that is not
// loopback in the insecure one.
func TestTheBrowserChecks(t *testing.T) {
	l, _ := newLocal(t, pathEnv())
	_, insecure := serve(t, l, WithInsecureNoAuth())
	_, authed := serve(t, l, WithAuthenticator(tokens()))
	port := insecure[strings.LastIndex(insecure, ":"):]
	json := map[string]string{"Content-Type": "application/json"}
	with := func(k, v string) map[string]string {
		return map[string]string{"Content-Type": "application/json", k: v}
	}
	type req struct {
		name, method, route, host, body string
		header                          map[string]string
		status                          int
		code                            string
	}
	common := []req{
		{"an Origin", "GET", "/v1/info", "", "", with("Origin", "http://evil.example"), 403, codeForbidden},
		{"an Origin of null", "GET", "/v1/info", "", "", with("Origin", "null"), 403, codeForbidden},
		{"an Origin of its own", "GET", "/v1/info", "", "", with("Origin", strings.TrimSuffix(insecure, "/")), 403, codeForbidden},
		{"cross-site", "GET", "/v1/info", "", "", with("Sec-Fetch-Site", "cross-site"), 403, codeForbidden},
		{"same-site", "GET", "/v1/info", "", "", with("Sec-Fetch-Site", "same-site"), 403, codeForbidden},
		{"same-origin", "GET", "/v1/info", "", "", with("Sec-Fetch-Site", "same-origin"), 200, ""},
		{"none", "GET", "/v1/info", "", "", with("Sec-Fetch-Site", "none"), 200, ""},
		{"exec as text/plain", "POST", "/v1/exec", "", `{"args":["true"]}`, map[string]string{"Content-Type": "text/plain"}, 415, codeMediaType},
		{"exec untyped", "POST", "/v1/exec", "", `{"args":["true"]}`, nil, 415, codeMediaType},
		{"exec as form", "POST", "/v1/exec", "", `{"args":["true"]}`, map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, 415, codeMediaType},
		{"exec as JSON with a charset", "POST", "/v1/exec", "", `{"args":["true"]}`, map[string]string{"Content-Type": "application/json; charset=utf-8"}, 200, ""},
		{"a session as text/plain", "POST", "/v1/sessions", "", `{}`, map[string]string{"Content-Type": "text/plain"}, 415, codeMediaType},
		{"stdin as text/plain", "POST", "/v1/sessions/S/processes/P/stdin", "", "x", map[string]string{"Content-Type": "text/plain"}, 415, codeMediaType},
		{"a signal as text/plain", "POST", "/v1/sessions/S/processes/P/signal", "", `{"signal":"KILL"}`, map[string]string{"Content-Type": "text/plain"}, 415, codeMediaType},
		{"a write as text/plain", "PUT", "/v1/fs/file?path=x&perm=420", "", "x", map[string]string{"Content-Type": "text/plain"}, 415, codeMediaType},
	}
	for _, mode := range []struct {
		name, url, token string
		extra            []req
	}{
		{"insecure", insecure, "", []req{
			{"a rebound Host", "GET", "/v1/info", "evil.example" + port, "", json, 403, codeForbidden},
			{"a Host with no port", "GET", "/v1/info", "evil.example", "", json, 403, codeForbidden},
			{"a private address", "GET", "/v1/info", "192.168.1.2" + port, "", json, 403, codeForbidden},
			{"localhost", "GET", "/v1/info", "localhost" + port, "", json, 200, ""},
			{"LOCALHOST", "GET", "/v1/info", "LOCALHOST" + port, "", json, 200, ""},
			{"127.0.0.2", "GET", "/v1/info", "127.0.0.2" + port, "", json, 200, ""},
			{"::1", "GET", "/v1/info", "[::1]" + port, "", json, 200, ""},
		}},
		{"authenticated", authed, tokAll, []req{
			{"any Host", "GET", "/v1/info", "workspace.example" + port, "", json, 200, ""},
		}},
	} {
		for _, r := range append(append([]req{}, common...), mode.extra...) {
			status, code := send(t, r.method, mode.url+r.route, r.host, mode.token, r.body, r.header)
			if status != r.status || code != r.code {
				t.Errorf("%s, %s: %d %q, want %d %q", mode.name, r.name, status, code, r.status, r.code)
			}
		}
	}
}
