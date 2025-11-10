package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/Oscariremma/tda596-distsys/lab1/httpCommon"
)

func main() {
	port := httpCommon.GetPort("8080")
	httpCommon.RunServer(port, handleConnection)
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		fmt.Println("Error reading HTTP request:", err)
		conn.Write(httpCommon.ResponseBadRequest)
		return
	}

	if req.Method != "GET" {
		fmt.Println("Unsupported HTTP method:", req.Method)
		conn.Write(httpCommon.ResponseNotImplemented)
		return
	}

	if !req.URL.IsAbs() {
		fmt.Println("Non-absolute URL in request:", req.URL)
		conn.Write(httpCommon.ResponseBadRequest)
		return
	}

	handleGetRequest(req, conn)
}

func handleGetRequest(req *http.Request, responseWriter io.Writer) {
	fmt.Println("Handling GET request for URL:", req.URL)

	targetConn, err := dialTarget(req.URL.Host)
	if err != nil {
		fmt.Println("Error connecting to target server:", err)
		responseWriter.Write(httpCommon.ResponseInternalServerError)
		return
	}
	defer targetConn.Close()

	prepareProxyRequest(req)

	if err := req.Write(targetConn); err != nil {
		fmt.Println("Error forwarding request to target server:", err)
		responseWriter.Write(httpCommon.ResponseInternalServerError)
		return
	}

	resp, err := http.ReadResponse(bufio.NewReader(targetConn), req)
	if err != nil {
		fmt.Println("Error reading response from target server:", err)
		responseWriter.Write(httpCommon.ResponseInternalServerError)
		return
	}
	defer resp.Body.Close()

	if err := resp.Write(responseWriter); err != nil {
		fmt.Println("Error writing response back to client:", err)
		return
	}

	fmt.Println("Successfully proxied GET request for URL:", req.URL)
}

func dialTarget(host string) (net.Conn, error) {
	targetHost := host
	if _, _, err := net.SplitHostPort(targetHost); err != nil {
		targetHost = net.JoinHostPort(targetHost, "80")
	}
	return net.Dial("tcp", targetHost)
}

func prepareProxyRequest(req *http.Request) {
	req.RequestURI = ""
	req.Header.Del("Proxy-Connection")
	req.URL.Scheme = ""
	req.Header.Add("Host", req.URL.Host)
	req.URL.Host = ""
}
