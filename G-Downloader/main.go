package main

import (
	"context"
	"log"
	"os"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"g-downloader/tools"
)

func main() {
	// Create a new MCP server
	s := server.NewMCPServer(
		"G-Downloader",
		"1.0.0",
		server.WithLogging(),
	)

	// Register detect_resource tool
	detectResourceTool := mcp.NewTool("detect_resource",
		mcp.WithDescription("Performs HTTP HEAD request to detect resource information including size, range support, and suggested filename"),
		mcp.WithString("url",
			mcp.Required(),
			mcp.Description("The URL of the resource to detect"),
		),
		mcp.WithString("cookies",
			mcp.Description("Optional cookies to include in the request"),
		),
		mcp.WithString("user_agent",
			mcp.Description("Optional User-Agent header"),
		),
	)
	s.AddTool(detectResourceTool, tools.DetectResourceHandler)

	// Register concurrent_download tool
	concurrentDownloadTool := mcp.NewTool("concurrent_download",
		mcp.WithDescription("Downloads a file using multiple concurrent connections with range requests"),
		mcp.WithString("url",
			mcp.Required(),
			mcp.Description("The URL of the file to download"),
		),
		mcp.WithString("dest_path",
			mcp.Required(),
			mcp.Description("The destination path where the file will be saved"),
		),
		mcp.WithNumber("threads",
			mcp.Required(),
			mcp.Description("Number of concurrent download threads (1-16)"),
		),
		mcp.WithString("cookies",
			mcp.Description("Optional cookies to include in the request"),
		),
		mcp.WithString("user_agent",
			mcp.Description("Optional User-Agent header"),
		),
	)
	s.AddTool(concurrentDownloadTool, tools.ConcurrentDownloadHandler)

	// Register adaptive_download tool (TCP-like congestion control)
	adaptiveDownloadTool := mcp.NewTool("adaptive_download",
		mcp.WithDescription("Downloads a file using TCP-like congestion control with binary tree segmentation. Automatically adjusts concurrency based on connection quality."),
		mcp.WithString("url",
			mcp.Required(),
			mcp.Description("The URL of the file to download"),
		),
		mcp.WithString("dest_path",
			mcp.Required(),
			mcp.Description("The destination path where the file will be saved"),
		),
		mcp.WithNumber("initial_cwnd",
			mcp.Description("Initial congestion window size (default: 1)"),
		),
		mcp.WithNumber("max_cwnd",
			mcp.Description("Maximum congestion window size (default: 16)"),
		),
		mcp.WithString("cookies",
			mcp.Description("Optional cookies to include in the request"),
		),
		mcp.WithString("user_agent",
			mcp.Description("Optional User-Agent header"),
		),
	)
	s.AddTool(adaptiveDownloadTool, tools.AdaptiveDownloadHandler)

	// Create stdio transport and start the server
	log.SetOutput(os.Stderr) // Redirect logs to stderr to not interfere with stdio
	if err := server.ServeStdio(s, server.WithStdioContextFunc(func(ctx context.Context) context.Context {
		return ctx
	})); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
