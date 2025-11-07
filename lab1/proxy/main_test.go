package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

// startTestProxy starts a TCP listener on a random local port and accepts connections.
// For every accepted connection it calls handleConnection(conn) in a new goroutine.
// It returns the address to connect to and a stop function that closes the listener.
func startTestProxy(t *testing.T) (addr string, stop func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start test proxy listener: %v", err)
	}

	var wg sync.WaitGroup
	stopped := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stopped:
					return
				default:
					// Accept error while not stopped: report and return
					return
				}
			}
			// Each connection handled in its own goroutine (as the real proxy should).
			go handleConnection(conn)
		}
	}()

	stop = func() {
		// Closing the listener will break the accept loop.
		close(stopped)
		ln.Close()
		wg.Wait()
	}

	return ln.Addr().String(), stop
}

// newProxiedClient returns an http.Client that sends requests via the proxyAddr (e.g. "127.0.0.1:12345").
func newProxiedClient(proxyAddr string) *http.Client {
	proxyURL, _ := url.Parse("http://" + proxyAddr)
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		// Keep defaults otherwise
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}
}

func TestProxyForwardsGET(t *testing.T) {
	// Origin server: returns 200 and body
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello-from-origin"))
	}))
	defer origin.Close()

	proxyAddr, stop := startTestProxy(t)
	defer stop()

	client := newProxiedClient(proxyAddr)

	// Perform GET to the origin URL but using the client configured to use the proxy.
	resp, err := client.Get(origin.URL)
	if err != nil {
		t.Fatalf("GET via proxy failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK from origin via proxy, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello-from-origin" {
		t.Fatalf("unexpected body from origin via proxy: %q", string(body))
	}
}

func TestProxyReturns501ForNonGET(t *testing.T) {
	// Origin server should not be contacted for non-GET by a correct proxy that rejects methods.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot) // should not be seen
		w.Write([]byte("origin should not be called"))
	}))
	defer origin.Close()

	proxyAddr, stop := startTestProxy(t)
	defer stop()

	client := newProxiedClient(proxyAddr)

	// Create a POST request and send via proxy.
	req, _ := http.NewRequest("POST", origin.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST via proxy failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		// The proxy is expected to reply 501 for non-GET methods. If the proxy forwards the request
		// (incorrectly), the origin would likely return another status; test should fail in that case.
		t.Fatalf("expected 501 Not Implemented for non-GET, got %d", resp.StatusCode)
	}
}

func TestProxyHandlesConcurrentRequests(t *testing.T) {
	// Origin server counts concurrent requests and responds after a small sleep to increase overlap.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// small delay to allow concurrency
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
	}))
	defer origin.Close()

	proxyAddr, stop := startTestProxy(t)
	defer stop()

	client := newProxiedClient(proxyAddr)

	const N = 8
	var wg sync.WaitGroup
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			resp, err := client.Get(origin.URL)
			if err != nil {
				errs <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatalf("concurrent request failed: %v", e)
		}
	}
}
