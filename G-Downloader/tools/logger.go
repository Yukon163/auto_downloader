package tools

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DownloadLogger handles logging for download operations
type DownloadLogger struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
}

var (
	globalLogger *DownloadLogger
	loggerOnce   sync.Once
)

// GetLogger returns the global download logger
func GetLogger() *DownloadLogger {
	loggerOnce.Do(func() {
		// Create log directory in user's home
		homeDir, err := os.UserHomeDir()
		if err != nil {
			homeDir = "."
		}
		logDir := filepath.Join(homeDir, ".auto_downloader", "logs")
		os.MkdirAll(logDir, 0755)

		// Create log file with date
		logFile := filepath.Join(logDir, fmt.Sprintf("download_%s.log", time.Now().Format("2006-01-02")))
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			// Fallback to stderr if file creation fails
			globalLogger = &DownloadLogger{filePath: logFile}
			return
		}

		globalLogger = &DownloadLogger{
			file:     file,
			filePath: logFile,
		}
	})
	return globalLogger
}

// Log writes a log entry with timestamp
func (l *DownloadLogger) Log(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] %s\n", timestamp, message)

	if l.file != nil {
		l.file.WriteString(logLine)
	}
	// Also write to stderr for debugging
	fmt.Fprint(os.Stderr, logLine)
}

// LogDownloadStart logs the start of a download
func (l *DownloadLogger) LogDownloadStart(url string, fileSize int64, acceptRanges bool) {
	l.Log("=== DOWNLOAD START ===")
	l.Log("URL: %s", url)
	l.Log("File Size: %.2f MB (%d bytes)", float64(fileSize)/(1024*1024), fileSize)
	l.Log("Range Support: %v", acceptRanges)
}

// LogCwndChange logs congestion window changes
func (l *DownloadLogger) LogCwndChange(oldCwnd, newCwnd int, phase string) {
	l.Log("CWND Change: %d -> %d (%s)", oldCwnd, newCwnd, phase)
}

// LogSegmentSplit logs when a segment is split
func (l *DownloadLogger) LogSegmentSplit(originalStart, originalSize, newStart, newSize int64, totalSegments int) {
	l.Log("Segment Split: [%d, size:%d] -> [%d, size:%d] + [%d, size:%d] (total: %d segments)",
		originalStart, originalSize+newSize,
		originalStart, originalSize,
		newStart, newSize,
		totalSegments)
}

// LogSegmentComplete logs when a segment download completes
func (l *DownloadLogger) LogSegmentComplete(start, size int64, duration time.Duration) {
	speedMBps := float64(size) / duration.Seconds() / (1024 * 1024)
	l.Log("Segment Complete: [%d, size:%d] in %.2fs (%.2f MB/s)",
		start, size, duration.Seconds(), speedMBps)
}

// LogSegmentFailed logs when a segment download fails
func (l *DownloadLogger) LogSegmentFailed(start, size int64, err error) {
	l.Log("Segment FAILED: [%d, size:%d] - %v", start, size, err)
}

// LogRoundComplete logs the end of a download round
func (l *DownloadLogger) LogRoundComplete(round int, cwnd int, completedSegments, totalSegments int, bytesDownloaded int64, elapsed time.Duration) {
	speedMBps := float64(bytesDownloaded) / elapsed.Seconds() / (1024 * 1024)
	l.Log("Round %d Complete: cwnd=%d, segments=%d/%d, speed=%.2f MB/s",
		round, cwnd, completedSegments, totalSegments, speedMBps)
}

// LogDownloadComplete logs the final download result
func (l *DownloadLogger) LogDownloadComplete(bytes int64, elapsed time.Duration, segments int, finalCwnd int) {
	speedMBps := float64(bytes) / elapsed.Seconds() / (1024 * 1024)
	l.Log("=== DOWNLOAD COMPLETE ===")
	l.Log("Total: %.2f MB in %.2fs", float64(bytes)/(1024*1024), elapsed.Seconds())
	l.Log("Average Speed: %.2f MB/s", speedMBps)
	l.Log("Final Segments: %d", segments)
	l.Log("Final CWND: %d", finalCwnd)
	l.Log("========================")
}

// LogDownloadError logs a download error
func (l *DownloadLogger) LogDownloadError(err string) {
	l.Log("=== DOWNLOAD ERROR ===")
	l.Log("Error: %s", err)
	l.Log("======================")
}

// GetLogPath returns the path to the log file
func (l *DownloadLogger) GetLogPath() string {
	return l.filePath
}
