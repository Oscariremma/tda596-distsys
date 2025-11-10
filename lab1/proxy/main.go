package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
)

type HttpResponse []byte

var (
	BadRequest          HttpResponse = []byte("HTTP/1.1 400 Bad Request\r\n\r\n")
	NotImplemented      HttpResponse = []byte("HTTP/1.1 501 Not Implemented\r\n\r\n")
	InternalServerError HttpResponse = []byte("HTTP/1.1 500 Internal Server Error\r\n\r\n")
)

func main() {
	port := getPort()
	run(port)
}

func getPort() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}
	return "8080"
}

func run(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
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

	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		fmt.Println("Error reading HTTP request:", err)
		conn.Write(BadRequest)
		return
	}

	if req.Method != "GET" {
		fmt.Println("Unsupported HTTP method:", req.Method)
		conn.Write(NotImplemented)
		return
	}

	if !req.URL.IsAbs() {
		fmt.Println("Non-absolute URL in request:", req.URL)
		conn.Write(BadRequest)
		return
	}

	handleGetRequest(req, conn)
}

func handleGetRequest(req *http.Request, responseWriter io.Writer) {
	fmt.Println("Handling GET request for URL:", req.URL)

	targetConn, err := dialTarget(req.URL.Host)
	if err != nil {
		fmt.Println("Error connecting to target server:", err)
		responseWriter.Write(InternalServerError)
		return
	}
	defer targetConn.Close()

	prepareProxyRequest(req)

	if err := req.Write(targetConn); err != nil {
		fmt.Println("Error forwarding request to target server:", err)
		responseWriter.Write(InternalServerError)
		return
	}

	resp, err := http.ReadResponse(bufio.NewReader(targetConn), req)
	if err != nil {
		fmt.Println("Error reading response from target server:", err)
		responseWriter.Write(InternalServerError)
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
