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
	"sync/atomic"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

/*
Dynamic Connection Splitting (Active Work Stealing) Algorithm

Core concepts:
1. Supervisor Loop: Monitors active workers every 500ms.
2. Active Splitting: If active_workers < cwnd, find the worker with the largest remaining bytes.
   - Truncate its "StopAt" (atomic) to `Current + Remaining/2`.
   - Launch a new worker for the second half.
3. Workers: Check `Current >= StopAt` during download and stop early if active split occurred.
*/

// AdaptiveDownloadResult represents the result of the download
type AdaptiveDownloadResult struct {
	Status       string  `json:"status"`
	TimeElapsed  float64 `json:"time_elapsed"`
	BytesTotal   int64   `json:"bytes_total,omitempty"`
	SegmentCount int     `json:"segment_count,omitempty"` // For compatibility, number of splits performed
	FinalCwnd    int     `json:"final_cwnd,omitempty"`
	Error        string  `json:"error,omitempty"`
}

// AdaptiveDownloadArgs represents the arguments
type AdaptiveDownloadArgs struct {
	URL         string `json:"url"`
	DestPath    string `json:"dest_path"`
	Cookies     string `json:"cookies"`
	UserAgent   string `json:"user_agent"`
	InitialCwnd int    `json:"initial_cwnd"`
	MaxCwnd     int    `json:"max_cwnd"`
	Size        int64  `json:"size"`
}

// Worker represents an active download task
type Worker struct {
	ID        int
	URL       string
	Start     int64
	End       int64 // Original End (or current target end)
	Current   int64 // Atomically updated: current write position
	StopAt    int64 // Atomically updated: active stop limit (can be reduced by supervisor)
	Completed bool
}

// DownloadManager orchestrates the download
type DownloadManager struct {
	mu            sync.Mutex
	fileSize      int64
	activeWorkers []*Worker
	completed     int64 // Number of completed segments (not bytes)
	totalSegments int   // Total segments created
	file          *os.File
	args          AdaptiveDownloadArgs
	ctx           context.Context
	cancel        context.CancelFunc

	// Error handling
	errChan  chan error
	firstErr error
}

func NewDownloadManager(ctx context.Context, args AdaptiveDownloadArgs, fileSize int64, file *os.File) *DownloadManager {
	ctx, cancel := context.WithCancel(ctx)
	return &DownloadManager{
		fileSize:      fileSize,
		activeWorkers: make([]*Worker, 0),
		file:          file,
		args:          args,
		ctx:           ctx,
		cancel:        cancel,
		errChan:       make(chan error, 1),
	}
}

// Start begins the download process
func (dm *DownloadManager) Start() AdaptiveDownloadResult {
	startTime := time.Now()

	// Initial worker for the whole file
	initialWorker := &Worker{
		ID:      1,
		URL:     dm.args.URL,
		Start:   0,
		End:     dm.fileSize,
		Current: 0,
		StopAt:  dm.fileSize,
	}

	dm.addWorker(initialWorker)
	go dm.runWorker(initialWorker)

	// Supervisor Loop
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err := <-dm.errChan:
			dm.cancel()
			return AdaptiveDownloadResult{
				Status:      "error",
				Error:       err.Error(),
				TimeElapsed: time.Since(startTime).Seconds(),
			}
		case <-dm.ctx.Done():
			return AdaptiveDownloadResult{
				Status:      "error",
				Error:       "download cancelled",
				TimeElapsed: time.Since(startTime).Seconds(),
			}
		case <-ticker.C:
			if dm.checkCompletion() {
				return AdaptiveDownloadResult{
					Status:       "success",
					TimeElapsed:  time.Since(startTime).Seconds(),
					BytesTotal:   dm.fileSize,
					SegmentCount: dm.totalSegments,
					FinalCwnd:    len(dm.activeWorkers), // Rough estimate
				}
			}
			dm.supervise()
		}
	}
}

// checkCompletion checks if all bytes are downloaded
// In this simplified model, we track workers. If active workers list is empty + no errors, we are done?
// Ideally we should track bytes.
// Actually, simpler: if all Created segments are Completed.
// But segments are dynamic.
// Let's rely on: if activeWorkers is empty and we covered the range.
// But better: Supervisor checks if activeWorkers is empty. If so, and no error, we are done.
func (dm *DownloadManager) checkCompletion() bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return len(dm.activeWorkers) == 0
}

// supervise checks if we should split any workers
func (dm *DownloadManager) supervise() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	activeCount := len(dm.activeWorkers)
	maxCwnd := dm.args.MaxCwnd
	if maxCwnd <= 0 {
		maxCwnd = 16
	}

	// We can add more workers if we haven't reached maxCwnd
	if activeCount < maxCwnd {
		// Find candidate to split (largest remaining bytes)
		var candidate *Worker
		var maxRemaining int64 = 0

		for _, w := range dm.activeWorkers {
			current := atomic.LoadInt64(&w.Current)
			stopAt := atomic.LoadInt64(&w.StopAt)
			remaining := stopAt - current

			// Only split if remaining is large enough (e.g. > 10MB)
			// AND it's significantly larger than others? Just max is fine.
			if remaining > 10*1024*1024 && remaining > maxRemaining {
				maxRemaining = remaining
				candidate = w
			}
		}

		if candidate != nil {
			dm.splitWorker(candidate)
		}
	}
}

// splitWorker splits a worker into two
func (dm *DownloadManager) splitWorker(w *Worker) {
	current := atomic.LoadInt64(&w.Current)
	stopAt := atomic.LoadInt64(&w.StopAt)

	// Calculate split point (midpoint of remaining)
	remaining := stopAt - current
	splitPoint := current + (remaining / 2)

	// Ensure split point is aligned? Not strictly necessary for `os.WriteAt`.

	// Create new worker for the second half
	newWorker := &Worker{
		ID:      dm.totalSegments + 2, // Simple ID generation
		URL:     dm.args.URL,
		Start:   splitPoint,
		End:     stopAt,     // Take the original stopAt
		Current: splitPoint, // Starts at split point
		StopAt:  stopAt,
	}

	// Update old worker to stop at split point
	atomic.StoreInt64(&w.StopAt, splitPoint)

	GetLogger().Log("Splitting Worker %d: OldRange[%d-%d] -> NewStop[%d]. New Worker %d taking [%d-%d]",
		w.ID, w.Start, stopAt, splitPoint, newWorker.ID, splitPoint, stopAt)

	dm.activeWorkers = append(dm.activeWorkers, newWorker)
	dm.totalSegments++

	go dm.runWorker(newWorker)
}

func (dm *DownloadManager) addWorker(w *Worker) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.activeWorkers = append(dm.activeWorkers, w)
	dm.totalSegments++
}

func (dm *DownloadManager) removeWorker(w *Worker) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	newActive := make([]*Worker, 0, len(dm.activeWorkers))
	for _, active := range dm.activeWorkers {
		if active != w {
			newActive = append(newActive, active)
		}
	}
	dm.activeWorkers = newActive
}

func (dm *DownloadManager) runWorker(w *Worker) {
	defer dm.removeWorker(w)

	// We use w.StopAt as the REQUESTED end, but realizing that HTTP range is inclusive
	// and we might want to request *more* and stop early, OR just request the current StopAt.
	// Since StopAt changes dynamically, we can't request "Until StopAt" because StopAt might shrink!
	// Wait, if we shrink StopAt, the HTTP request we *already sent* is fine, we just stop reading.
	// But for the NEW worker, we need a known range.
	// The problem is: what if we request Start-End (End=FileSize), but then we split it?
	// That works. The old worker just stops reading.
	// So we should request Start-End (where End is the *original* intended end, or even FileSize-1).
	// Let's request Start-w.End. w.End is the *max* it could ever go to.
	// w.StopAt is the *current* limit.

	reqEnd := w.End
	if reqEnd > dm.fileSize {
		reqEnd = dm.fileSize
	}

	// Ranges are inclusive.
	rangeStart := w.Start
	rangeEnd := reqEnd - 1

	// Use a loop to retry on failure?
	// For "TCP-like", we should retry.
	// But here simplicity: if fail, report error.

	GetLogger().Log("Worker %d starting: %d - %d", w.ID, rangeStart, rangeEnd)

	client := &http.Client{
		Timeout: 30 * time.Minute,
	}

	req, err := http.NewRequestWithContext(dm.ctx, "GET", w.URL, nil)
	if err != nil {
		dm.reportError(err)
		return
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd))
	if dm.args.Cookies != "" {
		req.Header.Set("Cookie", dm.args.Cookies)
	}
	if dm.args.UserAgent != "" {
		req.Header.Set("User-Agent", dm.args.UserAgent)
	} else {
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	}

	resp, err := client.Do(req)
	if err != nil {
		dm.reportError(err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		dm.reportError(fmt.Errorf("unexpected status code: %d", resp.StatusCode))
		return
	}

	buf := make([]byte, 32*1024) // 32KB buffer

	for {
		// Check active split condition
		current := atomic.LoadInt64(&w.Current)
		stopAt := atomic.LoadInt64(&w.StopAt)

		if current >= stopAt {
			// We reached our dynamic limit (someone stole the rest)
			GetLogger().Log("Worker %d reached dynamic stop at %d", w.ID, current)
			return
		}

		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			// Write to file
			_, writeErr := dm.file.WriteAt(buf[:n], current)
			if writeErr != nil {
				dm.reportError(writeErr)
				return
			}

			// Update progress
			atomic.AddInt64(&w.Current, int64(n))
		}

		if readErr != nil {
			if readErr == io.EOF {
				w.Completed = true
				return
			}
			dm.reportError(readErr)
			return
		}
	}
}

func (dm *DownloadManager) reportError(err error) {
	select {
	case dm.errChan <- err:
	default:
	}
}

// AdaptiveDownloadHandler handles the adaptive_download tool call
func AdaptiveDownloadHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args AdaptiveDownloadArgs
	if err := request.BindArguments(&args); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to parse arguments: %v", err)), nil
	}

	if args.URL == "" {
		return mcp.NewToolResultError("url parameter is required"), nil
	}
	if args.DestPath == "" {
		return mcp.NewToolResultError("dest_path parameter is required"), nil
	}

	if args.InitialCwnd <= 0 {
		args.InitialCwnd = 1
	}
	if args.MaxCwnd <= 0 {
		args.MaxCwnd = 16
	}

	result := adaptiveDownload(ctx, args)

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func adaptiveDownload(ctx context.Context, args AdaptiveDownloadArgs) AdaptiveDownloadResult {
	startTime := time.Now()
	result := AdaptiveDownloadResult{}

	// Detect resource
	detectResult := DetectResource(args.URL, args.Cookies, args.UserAgent)

	fileSize := args.Size
	if fileSize == 0 {
		fileSize = detectResult.Size
	} else if detectResult.Size > 0 && detectResult.Size != fileSize {
		GetLogger().Log("Size mismatch: provided=%d, detected=%d. Using provided size.", fileSize, detectResult.Size)
	}

	if fileSize == 0 {
		if detectResult.Error != "" {
			result.Status = "error"
			result.Error = detectResult.Error
			result.TimeElapsed = time.Since(startTime).Seconds()
			return result
		}
		result.Status = "error"
		result.Error = "could not determine file size"
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	// Create output file
	destDir := filepath.Dir(args.DestPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to create destination directory: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	outFile, err := os.OpenFile(args.DestPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to create output file: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}
	defer outFile.Close()

	if err := outFile.Truncate(fileSize); err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to allocate file space: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	GetLogger().LogDownloadStart(args.URL, fileSize, detectResult.AcceptRanges)

	// Single thread fallback
	if !detectResult.AcceptRanges {
		GetLogger().Log("No range support, using single download")
		// Reuse manager but with maxCwnd=1
		args.MaxCwnd = 1
	}

	dm := NewDownloadManager(ctx, args, fileSize, outFile)
	return dm.Start()
}
