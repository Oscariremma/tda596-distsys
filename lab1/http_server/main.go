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

	"github.com/Oscariremma/tda596-distsys/lab1/httpCommon"
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
	initDataDir()
	port := httpCommon.GetPort("8080")
	httpCommon.RunServerWithLimit(port, 10, handleConnection)
}

func initDataDir() {
	if err := os.Mkdir(baseDir, os.ModePerm); err != nil && !strings.Contains(err.Error(), "file exists") {
		log.Fatalf("Error creating directory: %s", err)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		fmt.Println("Error reading HTTP request:", err)
		conn.Write(httpCommon.ResponseBadRequest)
		return
	}

	switch req.Method {
	case http.MethodGet:
		handleGetRequest(req, conn)
	case http.MethodPost:
		handlePostRequest(req, conn)
	default:
		fmt.Println("Unsupported HTTP method:", req.Method)
		conn.Write(httpCommon.ResponseNotImplemented)
	}
}

func handleGetRequest(req *http.Request, responseWriter io.Writer) {
	fmt.Println("Handling GET request for URL:", req.URL)

	filePath, fileExt, err := validateAndBuildPath(req.URL.Path)
	if err != nil {
		fmt.Println(err)
		responseWriter.Write(httpCommon.ResponseBadRequest)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("File not found:", err)
		responseWriter.Write(httpCommon.ResponseNotFound)
		return
	}
	defer file.Close()

	mimeType := mimeTypes[fileExt]
	header := fmt.Sprintf("HTTP/1.1 200 ResponseOK\r\nContent-Type: %s\r\n\r\n", mimeType)
	responseWriter.Write([]byte(header))

	if _, err := io.Copy(responseWriter, file); err != nil {
		fmt.Println("Error sending file:", err)
	}
}

func handlePostRequest(req *http.Request, responseWriter io.Writer) {
	fmt.Println("Handling POST request for URL:", req.URL)

	filePath, _, err := validateAndBuildPath(req.URL.Path)
	if err != nil {
		fmt.Println(err)
		responseWriter.Write(httpCommon.ResponseBadRequest)
		return
	}

	file, err := os.Create(filePath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		responseWriter.Write(httpCommon.ResponseInternalServerError)
		return
	}
	defer file.Close()

	if _, err := io.Copy(file, req.Body); err != nil {
		fmt.Println("Error writing to file:", err)
		responseWriter.Write(httpCommon.ResponseInternalServerError)
		return
	}

	responseWriter.Write(httpCommon.ResponseOK)
}

func validateAndBuildPath(urlPath string) (string, string, error) {
	fileExt := getFileExtension(urlPath)
	if !isExtensionAllowed(fileExt) {
		return "", "", fmt.Errorf("file extension not allowed: %s", fileExt)
	}
	filePath := baseDir + strings.TrimPrefix(urlPath, "/")
	return filePath, fileExt, nil
}

func getFileExtension(path string) string {
	parts := strings.Split(path, ".")
	if len(parts) < 2 {
		return ""
	}
	return strings.ToLower(parts[len(parts)-1])
}

func isExtensionAllowed(ext string) bool {
	for _, allowed := range allowedExts {
		if ext == allowed {
			return true
		}
	}
	return false
}
