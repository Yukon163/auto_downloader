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
TCP-like Congestion Control Download Algorithm

Core concepts:
1. Binary Tree Segmentation: Start with full file, split into halves recursively
2. Congestion Window (cwnd): Number of concurrent downloads, dynamically adjusted
3. Slow Start: cwnd starts at 1, doubles on each success until ssthresh
4. Congestion Avoidance: After ssthresh, cwnd grows linearly
5. Fast Recovery: On timeout/failure, halve ssthresh and cwnd

Segment only tracks start position; end is calculated from next segment or file end.
*/

// AdaptiveDownloadResult represents the result of an adaptive download
type AdaptiveDownloadResult struct {
	Status       string  `json:"status"`
	TimeElapsed  float64 `json:"time_elapsed"`
	BytesTotal   int64   `json:"bytes_total,omitempty"`
	SegmentCount int     `json:"segment_count,omitempty"`
	FinalCwnd    int     `json:"final_cwnd,omitempty"`
	Error        string  `json:"error,omitempty"`
}

// AdaptiveDownloadArgs represents the arguments for adaptive download
type AdaptiveDownloadArgs struct {
	URL         string `json:"url"`
	DestPath    string `json:"dest_path"`
	Cookies     string `json:"cookies"`
	UserAgent   string `json:"user_agent"`
	InitialCwnd int    `json:"initial_cwnd"` // Optional: starting cwnd (default 1)
	MaxCwnd     int    `json:"max_cwnd"`     // Optional: max concurrent connections (default 16)
}

// Segment represents a download segment (only start position needed)
type Segment struct {
	Start     int64
	Size      int64 // Current segment size
	Completed bool
	Data      []byte
}

// CongestionController manages TCP-like congestion control
type CongestionController struct {
	mu          sync.Mutex
	cwnd        int  // Congestion window (number of concurrent segments)
	ssthresh    int  // Slow start threshold
	maxCwnd     int  // Maximum cwnd
	inSlowStart bool // Are we in slow start phase?

	successCount int32 // Atomic counter for successes in current window
	failureCount int32 // Atomic counter for failures
}

func NewCongestionController(initialCwnd, maxCwnd int) *CongestionController {
	if initialCwnd < 1 {
		initialCwnd = 1
	}
	if maxCwnd < 1 {
		maxCwnd = 16
	}
	cc := &CongestionController{
		cwnd:        initialCwnd,
		ssthresh:    maxCwnd / 2, // Initial ssthresh is half of max
		maxCwnd:     maxCwnd,
		inSlowStart: true,
	}
	GetLogger().Log("Congestion Controller initialized: cwnd=%d, ssthresh=%d, maxCwnd=%d", initialCwnd, cc.ssthresh, maxCwnd)
	return cc
}

func (cc *CongestionController) GetCwnd() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.cwnd
}

func (cc *CongestionController) OnSuccess() {
	atomic.AddInt32(&cc.successCount, 1)
}

func (cc *CongestionController) OnFailure() {
	atomic.AddInt32(&cc.failureCount, 1)
}

// AdjustWindow is called after a round of downloads
func (cc *CongestionController) AdjustWindow() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	oldCwnd := cc.cwnd
	successes := atomic.SwapInt32(&cc.successCount, 0)
	failures := atomic.SwapInt32(&cc.failureCount, 0)
	phase := "no change"

	if failures > 0 {
		// Congestion detected: multiplicative decrease
		cc.ssthresh = max(cc.cwnd/2, 1)
		cc.cwnd = max(cc.cwnd/2, 1)
		cc.inSlowStart = false
		phase = "fast recovery"
	} else if successes > 0 {
		if cc.inSlowStart {
			// Slow start: exponential increase
			cc.cwnd = min(cc.cwnd*2, cc.maxCwnd)
			if cc.cwnd >= cc.ssthresh {
				cc.inSlowStart = false
			}
			phase = "slow start"
		} else {
			// Congestion avoidance: linear increase
			cc.cwnd = min(cc.cwnd+1, cc.maxCwnd)
			phase = "congestion avoidance"
		}
	}

	if oldCwnd != cc.cwnd {
		GetLogger().LogCwndChange(oldCwnd, cc.cwnd, phase)
	}
}

// SegmentManager manages binary tree segmentation
type SegmentManager struct {
	mu             sync.Mutex
	fileSize       int64
	segments       []*Segment    // Sorted by start position
	pendingQueue   chan *Segment // Segments waiting to be downloaded
	completedCount int64
}

func NewSegmentManager(fileSize int64) *SegmentManager {
	sm := &SegmentManager{
		fileSize:     fileSize,
		segments:     make([]*Segment, 0),
		pendingQueue: make(chan *Segment, 1000),
	}

	// Start with single segment covering entire file
	initialSegment := &Segment{
		Start: 0,
		Size:  fileSize,
	}
	sm.segments = append(sm.segments, initialSegment)
	sm.pendingQueue <- initialSegment

	return sm
}

// SplitSegment performs binary split on a segment
func (sm *SegmentManager) SplitSegment(seg *Segment) *Segment {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Only split if segment is large enough (> 1MB)
	if seg.Size < 1024*1024 {
		return nil
	}

	// Binary split: create new segment at midpoint
	newSize := seg.Size / 2
	newSegment := &Segment{
		Start: seg.Start + newSize,
		Size:  seg.Size - newSize,
	}

	// Update original segment size
	seg.Size = newSize

	// Insert new segment into sorted list
	inserted := false
	newSegments := make([]*Segment, 0, len(sm.segments)+1)
	for _, s := range sm.segments {
		if !inserted && s.Start > newSegment.Start {
			newSegments = append(newSegments, newSegment)
			inserted = true
		}
		newSegments = append(newSegments, s)
	}
	if !inserted {
		newSegments = append(newSegments, newSegment)
	}
	sm.segments = newSegments

	GetLogger().LogSegmentSplit(seg.Start, seg.Size, newSegment.Start, newSegment.Size, len(sm.segments))

	return newSegment
}

// GetPendingSegment gets next segment to download
func (sm *SegmentManager) GetPendingSegment(timeout time.Duration) (*Segment, bool) {
	select {
	case seg := <-sm.pendingQueue:
		return seg, true
	case <-time.After(timeout):
		return nil, false
	}
}

// QueueSegment adds segment back to pending queue
func (sm *SegmentManager) QueueSegment(seg *Segment) {
	select {
	case sm.pendingQueue <- seg:
	default:
		// Queue full, try in goroutine
		go func() { sm.pendingQueue <- seg }()
	}
}

// MarkCompleted marks a segment as completed
func (sm *SegmentManager) MarkCompleted(seg *Segment, data []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	seg.Completed = true
	seg.Data = data
	sm.completedCount++
}

// AllCompleted checks if all segments are downloaded
func (sm *SegmentManager) AllCompleted() bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, seg := range sm.segments {
		if !seg.Completed {
			return false
		}
	}
	return true
}

// GetOrderedData returns all data in order
func (sm *SegmentManager) GetOrderedData() []byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	result := make([]byte, 0, sm.fileSize)
	for _, seg := range sm.segments {
		if seg.Data != nil {
			result = append(result, seg.Data...)
		}
	}
	return result
}

// GetSegmentCount returns the number of segments
func (sm *SegmentManager) GetSegmentCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.segments)
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

	// If no range support, do simple download
	if !detectResult.AcceptRanges {
		GetLogger().Log("No range support, using single download")
		return singleDownload(ctx, args, fileSize, startTime)
	}

	GetLogger().LogDownloadStart(args.URL, fileSize, detectResult.AcceptRanges)

	// Create destination directory
	destDir := filepath.Dir(args.DestPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to create destination directory: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	// Initialize congestion controller and segment manager
	cc := NewCongestionController(args.InitialCwnd, args.MaxCwnd)
	sm := NewSegmentManager(fileSize)

	// Main download loop
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	doneChan := make(chan struct{})

	go func() {
		for !sm.AllCompleted() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			cwnd := cc.GetCwnd()

			// Launch cwnd number of concurrent downloads
			for i := 0; i < cwnd; i++ {
				seg, ok := sm.GetPendingSegment(100 * time.Millisecond)
				if !ok {
					break
				}

				wg.Add(1)
				go func(segment *Segment) {
					defer wg.Done()
					segmentStart := time.Now()

					// Try to split segment for more parallelism
					if newSeg := sm.SplitSegment(segment); newSeg != nil {
						sm.QueueSegment(newSeg)
					}

					// Download this segment
					data, err := downloadSegment(ctx, args.URL, segment.Start, segment.Start+segment.Size-1, args.Cookies, args.UserAgent)

					if err != nil {
						cc.OnFailure()
						GetLogger().LogSegmentFailed(segment.Start, segment.Size, err)
						// Re-queue segment for retry
						sm.QueueSegment(segment)
						select {
						case errChan <- err:
						default:
						}
					} else {
						cc.OnSuccess()
						GetLogger().LogSegmentComplete(segment.Start, segment.Size, time.Since(segmentStart))
						sm.MarkCompleted(segment, data)
					}
				}(seg)
			}

			// Wait for this round to complete
			wg.Wait()

			// Adjust congestion window based on results
			cc.AdjustWindow()
		}
		close(doneChan)
	}()

	// Wait for completion or error
	select {
	case <-doneChan:
		// Success
	case <-ctx.Done():
		result.Status = "error"
		result.Error = "download cancelled"
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	// Write all data to file
	outFile, err := os.Create(args.DestPath)
	if err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to create output file: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}
	defer outFile.Close()

	data := sm.GetOrderedData()
	n, err := outFile.Write(data)
	if err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to write to file: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	result.Status = "success"
	result.TimeElapsed = time.Since(startTime).Seconds()
	result.BytesTotal = int64(n)
	result.SegmentCount = sm.GetSegmentCount()
	result.FinalCwnd = cc.GetCwnd()

	GetLogger().LogDownloadComplete(result.BytesTotal, time.Since(startTime), result.SegmentCount, result.FinalCwnd)

	return result
}

func singleDownload(ctx context.Context, args AdaptiveDownloadArgs, fileSize int64, startTime time.Time) AdaptiveDownloadResult {
	result := AdaptiveDownloadResult{}

	data, err := downloadSegment(ctx, args.URL, 0, fileSize-1, args.Cookies, args.UserAgent)
	if err != nil {
		result.Status = "error"
		result.Error = err.Error()
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	destDir := filepath.Dir(args.DestPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to create destination directory: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	if err := os.WriteFile(args.DestPath, data, 0644); err != nil {
		result.Status = "error"
		result.Error = fmt.Sprintf("failed to write file: %v", err)
		result.TimeElapsed = time.Since(startTime).Seconds()
		return result
	}

	result.Status = "success"
	result.TimeElapsed = time.Since(startTime).Seconds()
	result.BytesTotal = int64(len(data))
	result.SegmentCount = 1
	result.FinalCwnd = 1

	return result
}

func downloadSegment(ctx context.Context, url string, start, end int64, cookies, userAgent string) ([]byte, error) {
	client := &http.Client{
		Timeout: 2 * time.Minute, // Shorter timeout for adaptive retry
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	return data, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
