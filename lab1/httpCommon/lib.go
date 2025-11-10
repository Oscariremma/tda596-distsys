package httpCommon

import (
	"fmt"
	"log"
	"net"
	"os"
)

type HttpResponse []byte

// Common HTTP response constants
var (
	ResponseOK                  HttpResponse = []byte("HTTP/1.1 200 ResponseOK\r\n\r\n")
	ResponseNotFound            HttpResponse = []byte("HTTP/1.1 404 Not Found\r\n\r\n")
	ResponseBadRequest          HttpResponse = []byte("HTTP/1.1 400 Bad Request\r\n\r\n")
	ResponseNotImplemented      HttpResponse = []byte("HTTP/1.1 501 Not Implemented\r\n\r\n")
	ResponseInternalServerError HttpResponse = []byte("HTTP/1.1 500 Internal Server Error\r\n\r\n")
)

// GetPort returns the port from command line arguments or the default port
func GetPort(defaultPort string) string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}
	return defaultPort
}

// ConnectionHandler is a function type for handling connections
type ConnectionHandler func(net.Conn)

// RunServer starts a TCP server on the specified port with the given connection handler
func RunServer(port string, handler ConnectionHandler) {
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
		go handler(conn)
	}
}

// RunServerWithLimit starts a TCP server with a connection limit (semaphore)
func RunServerWithLimit(port string, maxConnections int, handler ConnectionHandler) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer ln.Close()

	fmt.Println("Server is listening on port", port)

	connCountSem := make(chan struct{}, maxConnections)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		connCountSem <- struct{}{}
		go func() {
			defer func() { <-connCountSem }()
			handler(conn)
		}()
	}
}
