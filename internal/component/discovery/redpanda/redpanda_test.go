package redpanda

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func clusterUUIDHandler(uuid string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster/uuid" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_uuid":"` + uuid + `"}`))
	}
}

// podAddrOf extracts the host and port from an httptest server's listener
// address, for building probeAdminAPI's podIP/AdminPort inputs directly
// rather than through its full URL (probeAdminAPI builds its own
// scheme://host:port URL internally).
func podAddrOf(t *testing.T, addr string) (host string, port int) {
	t.Helper()
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	portNum, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("parsing port %q: %v", p, err)
	}
	return h, portNum
}

// TestProbeAdminAPI_PlaintextOnlyServerDetectsHTTP covers a Redpanda
// cluster with no TLS on its admin API — probeAdminAPI's https attempt
// must fail cleanly and fall back to http, not error out just because the
// first scheme tried didn't work.
func TestProbeAdminAPI_PlaintextOnlyServerDetectsHTTP(t *testing.T) {
	srv := httptest.NewServer(clusterUUIDHandler("plaintext-uuid"))
	defer srv.Close()
	host, port := podAddrOf(t, srv.Listener.Addr().String())

	uuid, tlsEnabled, err := probeAdminAPI(host, Arguments{AdminPort: port})
	if err != nil {
		t.Fatalf("probeAdminAPI: %v", err)
	}
	if uuid != "plaintext-uuid" {
		t.Fatalf("expected uuid %q, got %q", "plaintext-uuid", uuid)
	}
	if tlsEnabled {
		t.Fatalf("expected tlsEnabled=false for a plaintext-only server")
	}
}

// TestProbeAdminAPI_TLSServerDetectsHTTPS covers a Redpanda cluster with
// TLS enabled (self-signed, matching typical in-cluster certs) — requires
// TLSSkipVerify since the test server's cert isn't in any trust store.
func TestProbeAdminAPI_TLSServerDetectsHTTPS(t *testing.T) {
	srv := httptest.NewTLSServer(clusterUUIDHandler("tls-uuid"))
	defer srv.Close()
	host, port := podAddrOf(t, srv.Listener.Addr().String())

	uuid, tlsEnabled, err := probeAdminAPI(host, Arguments{AdminPort: port, TLSSkipVerify: true})
	if err != nil {
		t.Fatalf("probeAdminAPI: %v", err)
	}
	if uuid != "tls-uuid" {
		t.Fatalf("expected uuid %q, got %q", "tls-uuid", uuid)
	}
	if !tlsEnabled {
		t.Fatalf("expected tlsEnabled=true for a TLS-only server")
	}
}

// TestProbeAdminAPI_TLSServerWithoutSkipVerifyFails covers the case where
// a self-signed cert is rejected (TLSSkipVerify: false) and there's no
// plaintext listener to fall back to either — both attempts should fail,
// not silently succeed via the wrong scheme.
func TestProbeAdminAPI_TLSServerWithoutSkipVerifyFails(t *testing.T) {
	srv := httptest.NewTLSServer(clusterUUIDHandler("tls-uuid"))
	defer srv.Close()
	host, port := podAddrOf(t, srv.Listener.Addr().String())

	_, _, err := probeAdminAPI(host, Arguments{AdminPort: port, TLSSkipVerify: false})
	if err == nil {
		t.Fatalf("expected an error: https should fail cert validation, and http has nothing to fall back to")
	}
}

// TestProbeAdminAPI_NothingListeningFails covers a pod whose admin port
// isn't reachable at all yet (e.g. still starting) — both attempts should
// fail with a combined error, not hang or panic.
func TestProbeAdminAPI_NothingListeningFails(t *testing.T) {
	// Grab a port and immediately release it so nothing is listening.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	host, port := podAddrOf(t, l.Addr().String())
	l.Close()

	_, _, err = probeAdminAPI(host, Arguments{AdminPort: port})
	if err == nil {
		t.Fatalf("expected an error when nothing is listening")
	}
}
