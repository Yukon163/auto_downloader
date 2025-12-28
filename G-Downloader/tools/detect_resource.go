package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// DetectResourceResult represents the result of a resource detection
type DetectResourceResult struct {
	Size              int64  `json:"size"`
	AcceptRanges      bool   `json:"accept_ranges"`
	SuggestedFilename string `json:"suggested_filename"`
	ContentType       string `json:"content_type"`
	Error             string `json:"error,omitempty"`
}

// DetectResourceHandler handles the detect_resource tool call
func DetectResourceHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Extract arguments using helper methods
	url := request.GetString("url", "")
	if url == "" {
		return mcp.NewToolResultError("url parameter is required"), nil
	}

	cookies := request.GetString("cookies", "")
	userAgent := request.GetString("user_agent", "")

	result := DetectResource(url, cookies, userAgent)

	// Convert result to JSON
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// DetectResource performs HTTP HEAD request to detect resource information
func DetectResource(url, cookies, userAgent string) DetectResourceResult {
	result := DetectResourceResult{}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Follow up to 10 redirects
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	// Create HEAD request
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		result.Error = fmt.Sprintf("failed to create request: %v", err)
		return result
	}

	// Set headers
	if cookies != "" {
		req.Header.Set("Cookie", cookies)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	} else {
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	}

	// Perform request
	resp, err := client.Do(req)
	if err != nil {
		result.Error = fmt.Sprintf("request failed: %v", err)
		return result
	}
	defer resp.Body.Close()

	// Check for error status codes
	if resp.StatusCode == 401 {
		result.Error = "unauthorized: authentication required (401)"
		return result
	}
	if resp.StatusCode == 403 {
		result.Error = "forbidden: access denied (403)"
		return result
	}
	if resp.StatusCode >= 400 {
		result.Error = fmt.Sprintf("HTTP error: %d %s", resp.StatusCode, resp.Status)
		return result
	}

	// Extract Content-Length
	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		if size, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
			result.Size = size
		}
	}

	// Check Accept-Ranges header
	acceptRanges := resp.Header.Get("Accept-Ranges")
	result.AcceptRanges = strings.ToLower(acceptRanges) == "bytes"

	// Extract Content-Type
	result.ContentType = resp.Header.Get("Content-Type")

	// Try to get filename from Content-Disposition header
	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		_, params, err := mime.ParseMediaType(contentDisposition)
		if err == nil {
			if filename, ok := params["filename"]; ok {
				result.SuggestedFilename = filename
			}
		}
	}

	// If no filename from Content-Disposition, try to extract from URL
	if result.SuggestedFilename == "" {
		result.SuggestedFilename = extractFilenameFromURL(url)
	}

	return result
}

func extractFilenameFromURL(url string) string {
	// Find the last path segment
	lastSlash := strings.LastIndex(url, "/")
	if lastSlash == -1 || lastSlash == len(url)-1 {
		return ""
	}

	filename := url[lastSlash+1:]

	// Remove query parameters
	if queryStart := strings.Index(filename, "?"); queryStart != -1 {
		filename = filename[:queryStart]
	}

	// Remove fragment
	if fragStart := strings.Index(filename, "#"); fragStart != -1 {
		filename = filename[:fragStart]
	}

	return filename
}
