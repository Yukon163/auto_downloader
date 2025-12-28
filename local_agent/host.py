"""
Python Agent - Native Messaging Host + Raw JSON-RPC Client
Receives download requests from Chrome Extension, uses Go downloader for large files.
Uses raw JSON-RPC over stdio instead of MCP SDK (for Python < 3.10 compatibility).
"""

import asyncio
import json
import struct
import sys
import os
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any


# Configuration
GO_DOWNLOADER_PATH = Path(__file__).parent.parent / "G-Downloader" / "go-downloader.exe"
DOWNLOAD_DIR = Path.home() / "Downloads"
LOG_DIR = Path.home() / ".auto_downloader" / "logs"
SIZE_THRESHOLD_MB = 50
MAX_CWND = 16  # Maximum congestion window (concurrent connections)

# Ensure log directory exists
LOG_DIR.mkdir(parents=True, exist_ok=True)

def get_log_file():
    """Get today's log file path."""
    from datetime import datetime
    return LOG_DIR / f"agent_{datetime.now().strftime('%Y-%m-%d')}.log"

def log(message: str):
    """Log to both stderr and log file."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_line = f"[{timestamp}] {message}"
    
    # Write to stderr
    print(log_line, file=sys.stderr, flush=True)
    
    # Write to log file
    try:
        with open(get_log_file(), "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception:
        pass  # Don't fail if logging fails


def get_native_message() -> Optional[dict]:
    """Read a message from Chrome's native messaging."""
    raw_length = sys.stdin.buffer.read(4)
    if not raw_length:
        return None
    message_length = struct.unpack("=I", raw_length)[0]
    message = sys.stdin.buffer.read(message_length).decode("utf-8")
    return json.loads(message)


def send_native_message(response: dict):
    """Send a message back to Chrome extension."""
    encoded = json.dumps(response).encode("utf-8")
    sys.stdout.buffer.write(struct.pack("=I", len(encoded)))
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()


class MCPClient:
    """Simple MCP client using raw JSON-RPC over stdio."""
    
    def __init__(self, executable_path: Path):
        self.executable_path = executable_path
        self.process: Optional[subprocess.Popen] = None
        self.request_id = 0
    
    def start(self):
        """Start the MCP server process."""
        self.process = subprocess.Popen(
            [str(self.executable_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )
        
        # Send initialize request
        init_response = self._send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "python-agent", "version": "1.0"}
        })
        
        if not init_response or "error" in init_response:
            raise Exception(f"Failed to initialize MCP: {init_response}")
        
        log(f"MCP initialized: {init_response.get('result', {}).get('serverInfo', {})}")
    
    def stop(self):
        """Stop the MCP server process."""
        if self.process:
            self.process.terminate()
            self.process.wait(timeout=5)
            self.process = None
    
    def _send_request(self, method: str, params: dict) -> dict:
        """Send a JSON-RPC request and read the response."""
        if not self.process:
            raise Exception("MCP process not started")
        
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params
        }
        
        request_line = json.dumps(request) + "\n"
        self.process.stdin.write(request_line.encode())
        self.process.stdin.flush()
        
        # Read response line
        response_line = self.process.stdout.readline().decode()
        if not response_line:
            return {"error": "No response from MCP server"}
        
        return json.loads(response_line)
    
    def call_tool(self, tool_name: str, arguments: dict) -> dict:
        """Call an MCP tool and return the result."""
        response = self._send_request("tools/call", {
            "name": tool_name,
            "arguments": arguments
        })
        
        if "error" in response:
            return {"error": response["error"].get("message", str(response["error"]))}
        
        result = response.get("result", {})
        content = result.get("content", [])
        
        if content and len(content) > 0:
            text = content[0].get("text", "{}")
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"error": f"Invalid JSON in response: {text}"}
        
        return {"error": "Empty response from tool"}


def detect_resource(client: MCPClient, url: str, cookies: str = "", user_agent: str = "") -> dict:
    """Call detect_resource tool on the Go MCP server."""
    return client.call_tool("detect_resource", {
        "url": url,
        "cookies": cookies,
        "user_agent": user_agent
    })


def adaptive_download(
    client: MCPClient, 
    url: str, 
    dest_path: str,
    cookies: str = "",
    user_agent: str = "",
    max_cwnd: int = 16
) -> dict:
    """Call adaptive_download tool on the Go MCP server.
    Uses TCP-like congestion control with binary tree segmentation.
    """
    return client.call_tool("adaptive_download", {
        "url": url,
        "dest_path": dest_path,
        "initial_cwnd": 1,  # Start with slow start
        "max_cwnd": max_cwnd,
        "cookies": cookies,
        "user_agent": user_agent
    })




def handle_download_request(message: dict) -> dict:
    """Process a download request from the Chrome extension."""
    url = message.get("url", "")
    cookies = message.get("cookies", "")
    user_agent = message.get("userAgent", "")
    suggested_filename = message.get("filename", "")
    chrome_total_bytes = message.get("totalBytes", 0)  # Chrome's detected file size
    
    if not url:
        return {"status": "error", "message": "No URL provided"}
    
    # Check if Go downloader exists
    if not GO_DOWNLOADER_PATH.exists():
        return {
            "status": "error", 
            "message": f"Go downloader not found at {GO_DOWNLOADER_PATH}",
            "action": "continue_native"
        }
    
    client = MCPClient(GO_DOWNLOADER_PATH)
    
    try:
        client.start()
        
        # Step 1: Detect resource
        detect_result = detect_resource(client, url, cookies, user_agent)
        
        if detect_result.get("error"):
            return {
                "status": "error",
                "message": detect_result["error"],
                "action": "continue_native"
            }
        
        file_size = detect_result.get("size", 0)
        accept_ranges = detect_result.get("accept_ranges", False)
        filename = detect_result.get("suggested_filename") or suggested_filename or "download"
        
        # Use Chrome's totalBytes as fallback if HEAD request returns wrong size
        # (common with auth-protected URLs where HEAD returns error page)
        if file_size < 1024 * 1024 and chrome_total_bytes > 1024 * 1024:  # Detected < 1MB but Chrome sees > 1MB
            log(f"HEAD returned {file_size} bytes, using Chrome's totalBytes: {chrome_total_bytes}")
            file_size = chrome_total_bytes
        
        size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
        
        log(f"Detected: size={size_mb:.1f}MB, accept_ranges={accept_ranges}, filename={filename}")
        
        # Step 2: Decision logic
        if size_mb < SIZE_THRESHOLD_MB or not accept_ranges:
            # Let Chrome handle it natively
            return {
                "status": "skip",
                "action": "continue_native",
                "reason": f"Size {size_mb:.1f}MB < {SIZE_THRESHOLD_MB}MB or no range support",
                "file_size": file_size,
                "accept_ranges": accept_ranges
            }
        
        # Step 3: Use adaptive download with TCP-like congestion control
        dest_path = str(DOWNLOAD_DIR / filename)
        
        # Ensure unique filename
        counter = 1
        base_dest = dest_path
        while os.path.exists(dest_path):
            name, ext = os.path.splitext(base_dest)
            dest_path = f"{name} ({counter}){ext}"
            counter += 1
        
        log(f"Starting adaptive download: {url} -> {dest_path}")
        
        download_result = adaptive_download(
            client, url, dest_path, cookies, user_agent, MAX_CWND
        )
        
        if download_result.get("status") == "success":
            return {
                "status": "success",
                "action": "downloaded",
                "file_path": dest_path,
                "file_size": file_size,
                "segments": download_result.get("segment_count", 0),
                "final_cwnd": download_result.get("final_cwnd", 1),
                "time_elapsed": download_result.get("time_elapsed", 0)
            }
        else:
            return {
                "status": "error",
                "action": "continue_native",
                "message": download_result.get("error", "Download failed")
            }
    
    except Exception as e:
        log(f"Error: {e}")
        return {
            "status": "error",
            "message": str(e),
            "action": "continue_native"
        }
    
    finally:
        client.stop()


def main():
    """Entry point for native messaging host."""
    # Ensure download directory exists
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    log("Auto Downloader Agent started")
    
    while True:
        message = get_native_message()
        if message is None:
            log("No more messages, exiting")
            break
        
        log(f"Received message: {message}")
        
        try:
            response = handle_download_request(message)
        except Exception as e:
            log(f"Unhandled error: {e}")
            response = {
                "status": "error",
                "message": str(e),
                "action": "continue_native"
            }
        
        log(f"Sending response: {response}")
        send_native_message(response)


if __name__ == "__main__":
    main()
