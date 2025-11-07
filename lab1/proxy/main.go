package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
)

type HttpResponse []byte

var (
	OK                  HttpResponse = []byte("HTTP/1.1 200 OK\r\n\r\n")
	NotFound            HttpResponse = []byte("HTTP/1.1 404 Not Found\r\n\r\n")
	BadRequest          HttpResponse = []byte("HTTP/1.1 400 Bad Request\r\n\r\n")
	NotImplemented      HttpResponse = []byte("HTTP/1.1 501 Not Implemented\r\n\r\n")
	InternalServerError HttpResponse = []byte("HTTP/1.1 500 Internal Server Error\r\n\r\n")
)

func main() {
	args := os.Args[1:]

	port := "8080" // default port
	if len(args) > 0 {
		port = args[0]
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}

	defer ln.Close()

	fmt.Println("Server is listening on port", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)

	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	httpReq, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Error reading HTTP request:", err)
		conn.Write(BadRequest)
		return
	}

	if httpReq.Method != "GET" {
		fmt.Println("Unsupported HTTP method:", httpReq.Method)
		conn.Write(NotImplemented)
		return
	}

	if !httpReq.URL.IsAbs() {
		// Non-absolute URL; cannot proxy
		fmt.Println("Non-absolute URL in request:", httpReq.URL)
		conn.Write(BadRequest)
		return
	}

	handleGetRequest(httpReq, conn)

}

func handleGetRequest(req *http.Request, respWriter io.Writer) {
	fmt.Println("Handling GET request for URL:", req.URL)

	targetHost := req.URL.Host
	if _, _, err := net.SplitHostPort(targetHost); err != nil {
		// No port specified, default to 80
		targetHost = net.JoinHostPort(targetHost, "80")
	}

	targetConn, err := net.Dial("tcp", targetHost)
	if err != nil {
		fmt.Println("Error connecting to target server:", err)
		respWriter.Write(InternalServerError)
		return
	}
	defer targetConn.Close()

	// Remove proxy-specific headers
	req.RequestURI = ""
	req.Header.Del("Proxy-Connection")
	req.URL.Scheme = ""
	req.Header.Add("Host", req.URL.Host)
	req.URL.Host = ""

	err = req.Write(targetConn)
	if err != nil {
		fmt.Println("Error forwarding request to target server:", err)
		respWriter.Write(InternalServerError)
		return
	}

	targetReader := bufio.NewReader(targetConn)
	resp, err := http.ReadResponse(targetReader, req)
	if err != nil {
		fmt.Println("Error reading response from target server:", err)
		respWriter.Write(InternalServerError)
		return
	}
	defer resp.Body.Close()

	err = resp.Write(respWriter)
	if err != nil {
		fmt.Println("Error writing response back to client:", err)
		return
	}

	fmt.Println("Successfully proxied GET request for URL:", req.URL)
}
