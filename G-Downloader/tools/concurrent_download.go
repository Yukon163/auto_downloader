package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// ConcurrentDownloadResult represents the result of a concurrent download
type ConcurrentDownloadResult struct {
	Status      string  `json:"status"`
	TimeElapsed float64 `json:"time_elapsed"`
	BytesTotal  int64   `json:"bytes_total,omitempty"`
	Error       string  `json:"error,omitempty"`
}

// chunkResult holds the result of downloading a single chunk
type chunkResult struct {
	index int
	data  []byte
	err   error
}

// ConcurrentDownloadArgs represents the arguments for concurrent download
type ConcurrentDownloadArgs struct {
	URL       string  `json:"url"`
	DestPath  string  `json:"dest_path"`
	Threads   float64 `json:"threads"`
	Cookies   string  `json:"cookies"`
	UserAgent string  `json:"user_agent"`
}

// ConcurrentDownloadHandler handles the concurrent_download tool call
func ConcurrentDownloadHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Extract arguments using BindArguments
	var args ConcurrentDownloadArgs
	if err := request.BindArguments(&args); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to parse arguments: %v", err)), nil
	}

	if args.URL == "" {
		return mcp.NewToolResultError("url parameter is required"), nil
	}
	if args.DestPath == "" {
		return mcp.NewToolResultError("dest_path parameter is required"), nil
	}

	threads := int(args.Threads)
	if threads < 1 {
		threads = 1
	}
	if threads > 16 {
		threads = 16
	}

	result := concurrentDownload(ctx, args.URL, args.DestPath, threads, args.Cookies, args.UserAgent)

	// Convert result to JSON
	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func concurrentDownload(ctx context.Context, url, destPath string, threads int, cookies, userAgent string) ConcurrentDownloadResult {
	startTime := time.Now()
	result := ConcurrentDownloadResult{}

	// First, detect the resource to get size and range support
	detectResult := DetectResource(url, cookies, userAgent)
	if detectResult.Error != "" {
		result.Status = "error"
		result.Error = detectResult.Error
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	fileSize := detectResult.Size
	if fileSize == 0 {
		result.Status = "error"
		result.Error = "could not determine file size"
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	// If server doesn't support ranges, fall back to single-threaded download
	if !detectResult.AcceptRanges {
		threads = 1
	}

	// Create destination directory if it doesn't exist
	destDir := filepath.Dir(destPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to create destination directory: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	// Calculate chunk sizes
	chunkSize := fileSize / int64(threads)
	chunks := make([]struct {
		start int64
		end   int64
	}, threads)

	for i := 0; i < threads; i++ {
		chunks[i].start = int64(i) * chunkSize
		if i == threads-1 {
			chunks[i].end = fileSize - 1
		} else {
			chunks[i].end = chunks[i].start + chunkSize - 1
		}
	}

	// Download chunks concurrently
	var wg sync.WaitGroup
	resultsChan := make(chan chunkResult, threads)

	for i, chunk := range chunks {
		wg.Add(1)
		go func(index int, start, end int64) {
			defer wg.Done()
			data, err := downloadChunk(ctx, url, start, end, cookies, userAgent)
			resultsChan <- chunkResult{index: index, data: data, err: err}
		}(i, chunk.start, chunk.end)
	}

	// Wait for all downloads to complete
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	chunkData := make([][]byte, threads)
	for res := range resultsChan {
		if res.err != nil {
			result.Status = "error"
			result.Error = fmt.Sprintf("chunk %d download failed: %v", res.index, res.err)
			result.TimeElapsed = time.Since(startTime).Seconds()
			return result
		}
		chunkData[res.index] = res.data
	}

	// Create the output file
	outFile, err := os.Create(destPath)
	if err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to create output file: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}
	defer outFile.Close()

	// Write chunks in order
	var totalWritten int64
	for _, data := range chunkData {
		n, err := outFile.Write(data)
		if err != nil {
			result.Status = "error"
			result.Error = fmt.Sprintf("failed to write to file: %v", err)
			result.TimeElapsed = time.Since(startTime).Seconds()
			return result
		}
		totalWritten += int64(n)
	}

	result.Status = "success"
	result.TimeElapsed = time.Since(startTime).Seconds()
	result.BytesTotal = totalWritten

	return result
}

func downloadChunk(ctx context.Context, url string, start, end int64, cookies, userAgent string) ([]byte, error) {
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Set Range header
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	// Set other headers
	if cookies != "" {
		req.Header.Set("Cookie", cookies)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	} else {
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Check for successful response (200 or 206 Partial Content)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read the response body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	return data, nil
}
