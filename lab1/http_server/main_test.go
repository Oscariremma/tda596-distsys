package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	labDir  = "./"
	dataDir = "./data"
	binName = "http_server_test_bin"
)

// buildServer builds the server binary in the lab directory and returns its path.
func buildServer(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", binName, "main.go")
	cmd.Dir = labDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build server binary: %v\noutput: %s", err, string(out))
	}
	// return absolute path so exec can run it directly
	p := filepath.Join(labDir, binName)
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}

// startServer starts the built binary on the given port and returns the Cmd and a cleanup func.
func startServer(t *testing.T, binPath string, port int) (*exec.Cmd, func()) {
	t.Helper()
	portStr := fmt.Sprintf("%d", port)
	cmd := exec.Command(binPath, portStr)
	cmd.Dir = labDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	cleanup := func() {
		_ = cmd.Process.Kill()
		cmd.Wait()
		// remove binary built for tests
		_ = os.Remove(binPath)
	}
	// wait for server to accept connections
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return cmd, cleanup
		}
		time.Sleep(50 * time.Millisecond)
	}
	cleanup()
	t.Fatalf("server did not start listening in time on %s", addr)
	return nil, nil
}

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not get free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func cleanupDataDir(t *testing.T) {
	t.Helper()
	os.RemoveAll(dataDir)
	// server will recreate it on start
}

func TestUploadAndGet(t *testing.T) {
	cleanupDataDir(t)
	bin := buildServer(t)
	port := getFreePort(t)
	cmd, cleanup := startServer(t, bin, port)
	defer cleanup()
	_ = cmd

	url := fmt.Sprintf("http://127.0.0.1:%d/test.txt", port)
	content := "hello world"
	// POST the file
	resp, err := http.Post(url, "text/plain", strings.NewReader(content))
	if err != nil {
		t.Fatalf("POST request failed: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 from POST, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// GET the file
	resp, err = http.Get(url)
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 from GET, got %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "text/plain" {
		t.Fatalf("expected Content-Type text/plain, got %q", ct)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading GET body failed: %v", err)
	}
	if string(b) != content {
		t.Fatalf("GET body mismatch: want %q got %q", content, string(b))
	}
}

func TestBadExtensionAndNotFound(t *testing.T) {
	cleanupDataDir(t)
	bin := buildServer(t)
	port := getFreePort(t)
	cmd, cleanup := startServer(t, bin, port)
	defer cleanup()
	_ = cmd

	// Bad extension on POST
	url := fmt.Sprintf("http://127.0.0.1:%d/file.exe", port)
	resp, err := http.Post(url, "application/octet-stream", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("POST bad ext failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 for bad extension, got %d", resp.StatusCode)
	}

	// Not found on GET
	url2 := fmt.Sprintf("http://127.0.0.1:%d/doesnotexist.txt", port)
	resp2, err := http.Get(url2)
	if err != nil {
		t.Fatalf("GET not found failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("expected 404 for missing file, got %d", resp2.StatusCode)
	}
}

func TestNotImplementedMethod(t *testing.T) {
	cleanupDataDir(t)
	bin := buildServer(t)
	port := getFreePort(t)
	cmd, cleanup := startServer(t, bin, port)
	defer cleanup()
	_ = cmd

	// Send a PUT request which should return 501
	client := &http.Client{}
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://127.0.0.1:%d/put.txt", port), strings.NewReader("x"))
	if err != nil {
		t.Fatalf("creating PUT request failed: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 501 {
		t.Fatalf("expected 501 for unsupported method, got %d", resp.StatusCode)
	}
}
