package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
)

type HttpResponse []byte

var (
	OK                  HttpResponse = []byte("HTTP/1.1 200 OK\r\n\r\n")
	NotFound            HttpResponse = []byte("HTTP/1.1 404 Not Found\r\n\r\n")
	BadRequest          HttpResponse = []byte("HTTP/1.1 400 Bad Request\r\n\r\n")
	NotImplemented      HttpResponse = []byte("HTTP/1.1 501 Not Implemented\r\n\r\n")
	InternalServerError HttpResponse = []byte("HTTP/1.1 500 Internal Server Error\r\n\r\n")
)

var allowedExts = []string{"txt", "html", "png", "jpg", "jpeg", "css", "gif"}

const baseDir = "./data/"

var mimeTypes = map[string]string{
	"txt":  "text/plain",
	"html": "text/html",
	"png":  "image/png",
	"jpg":  "image/jpeg",
	"jpeg": "image/jpeg",
	"css":  "text/css",
	"gif":  "image/gif",
}

func main() {
	args := os.Args[1:]

	err := os.Mkdir(baseDir, os.ModePerm)
	if err != nil {
		// to avoid unused variable warning
		if !strings.Contains(err.Error(), "file exists") {
			log.Fatalf("Error creating directory: %s", err)
		}
	}

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

	connCountSem := make(chan struct{}, 10) // limit to 10 concurrent connections

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		connCountSem <- struct{}{} // acquire semaphore

		go func() {
			defer func() { <-connCountSem }() // release semaphore
			handleConnection(conn)
			// no artificial sleep; handlers release immediately when done
		}()
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

	switch httpReq.Method {
	case http.MethodGet:
		handleGetRequest(httpReq, conn)
	case http.MethodPost:
		handlePostRequest(httpReq, conn)
	default:
		fmt.Println("Unsupported HTTP method:", httpReq.Method)
		conn.Write(NotImplemented)
	}
}

func handleGetRequest(req *http.Request, respWriter io.Writer) {

	fmt.Println("Handling GET request for URL:", req.URL)

	isAllowed, fileExt := validateFileExtension(req.URL.Path)

	if !isAllowed {
		fmt.Println("File extension not allowed:", fileExt)
		respWriter.Write(BadRequest)
		return
	}

	filePath := baseDir + strings.TrimPrefix(req.URL.Path, "/")
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("File not found:", err)
		respWriter.Write(NotFound)
		return
	}
	defer file.Close()

	mimeType, exists := mimeTypes[fileExt]
	if !exists {
		fmt.Println("MIME type not found for extension:", fileExt)
		respWriter.Write(InternalServerError)
		return
	}

	header := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: %s\r\n\r\n", mimeType)
	respWriter.Write([]byte(header))

	_, err = io.Copy(respWriter, file)
	if err != nil {
		fmt.Println("Error sending file:", err)
		respWriter.Write(InternalServerError)
		return
	}
}

func handlePostRequest(req *http.Request, respWriter io.Writer) {
	fmt.Println("Handling POST request for URL:", req.URL)

	isAllowed, fileExt := validateFileExtension(req.URL.Path)

	if !isAllowed {
		fmt.Println("File extension not allowed:", fileExt)
		respWriter.Write(BadRequest)
		return
	}

	filePath := baseDir + strings.TrimPrefix(req.URL.Path, "/")

	file, err := os.Create(filePath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		respWriter.Write(InternalServerError)
		return
	}
	defer file.Close()

	_, err = io.Copy(file, req.Body)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		respWriter.Write(InternalServerError)
		return
	}

	respWriter.Write(OK)
}

func validateFileExtension(filePath string) (bool, string) {
	pathParts := strings.Split(filePath, ".")
	if len(pathParts) < 2 {
		return false, ""
	}

	fileExt := pathParts[len(pathParts)-1]
	fileExt = strings.ToLower(fileExt)

	for _, ext := range allowedExts {
		if fileExt == ext {
			return true, fileExt
		}
	}
	return false, fileExt
}
