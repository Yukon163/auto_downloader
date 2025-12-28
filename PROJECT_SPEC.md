Project Spec: Intelligent Download Agent via MCP
1. Project Overview
Build a local intelligent download system that intercepts browser download requests, analyzes the file (size, server support), and dynamically decides whether to use a multi-threaded Go-based downloader via the Model Context Protocol (MCP).

2. Architecture Stack
Client (Trigger): Chrome Extension (Manifest V3) using Native Messaging.

Controller (Agent/Host): Python 3.10+ (using mcp SDK).

Worker (MCP Server): Go 1.21+ (Standalone Binary).

3. Component Specifications
A. The Worker (Go Downloader as MCP Server)
Path: ./go-downloader/ Role: Provides atomic tools to the Agent. Tools to Expose via MCP:

detect_resource(url: string) -> JSON

Performs HTTP HEAD.

Returns: { "size": int, "accept_ranges": bool, "suggested_filename": string }

concurrent_download(url: string, dest_path: string, threads: int) -> JSON

Implements the multi-threaded range-request logic.

Handles file merging and cleanup.

Returns: { "status": "success", "time_elapsed": float }

Constraints:

Use standard net/http.

Implement stdio transport for MCP communication.

Must handle 403/401 errors gracefully.

B. The Controller (Python Agent)
Path: ./agent/ Role: Native Messaging Host + MCP Client. Logic Flow:

Receive message from Chrome Extension: { "url": "...", "cookies": "..." }.

Initialize MCP Client and connect to Go Binary (stdio).

Decision Loop:

Call detect_resource.

Logic:

IF size < 50MB OR !accept_ranges: Signal Chrome to continue native download.

IF size >= 50MB: Calculate threads (e.g., size / 10MB, max 16).

Call concurrent_download.

Send "Success/Fail" status back to Chrome Extension.

C. The Client (Chrome Extension)
Path: ./chrome-ext/ Role: Interceptor. Permissions: downloads, nativeMessaging. Workflow:

chrome.downloads.onCreated -> Pause/Cancel native download.

Send url, cookies, userAgent to Native Host (Python Agent).

Wait for response. Show Notification on completion.

4. Implementation Steps for AI
Step 1 (Go): Write the Go CLI tool that runs as an MCP Server (using github.com/mark3labs/mcp-go or raw JSON-RPC on stdin/out).

Step 2 (Python): Write the host.py script. Use model-context-protocol SDK to connect to the Go binary. Implement the decision logic.

Step 3 (Manifest): Create manifest.json and background.js for Chrome. Register the Native Messaging Host in the OS registry (mock setup).

5. Key Algorithms (Go)
Range Header: Range: bytes={start}-{end}

Worker Pool: Use sync.WaitGroup for concurrent chunks.

Merge: Append chunks in order (os.OpenFile with O_APPEND)