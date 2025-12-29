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
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import time



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
    max_cwnd: int = 16,
    size: int = 0
) -> dict:
    """Call adaptive_download tool on the Go MCP server."""
    return client.call_tool("adaptive_download", {
        "url": url,
        "dest_path": dest_path,
        "initial_cwnd": 1,
        "max_cwnd": max_cwnd,
        "cookies": cookies,
        "user_agent": user_agent,
        "size": size
    })


class DownloadManagerUI:
    def __init__(self, client: MCPClient, download_info: dict, on_complete_callback):
        self.client = client
        self.download_info = download_info
        self.on_complete_callback = on_complete_callback
        self.download_thread = None
        self.monitor_thread = None
        self.stop_event = threading.Event()
        self.download_result = None
        self.log_file = get_log_file()
        
        # UI Components
        self.root = tk.Tk()
        self.root.title("Intelligent Download Manager")
        self.root.geometry("600x450")
        self.root.resizable(False, False)
        
        self.create_widgets()
        
        # Center window
        self.root.eval('tk::PlaceWindow . center')
        
    def create_widgets(self):
        # Header
        header_frame = ttk.Frame(self.root, padding="10")
        header_frame.pack(fill=tk.X)
        
        ttk.Label(header_frame, text="File URL:", font=("Arial", 9, "bold")).pack(anchor=tk.W)
        url_label = ttk.Label(header_frame, text=self.download_info['url'], foreground="blue", wraplength=580)
        url_label.pack(anchor=tk.W, pady=(0, 10))
        
        # Destination
        dest_frame = ttk.Frame(self.root, padding="10")
        dest_frame.pack(fill=tk.X)
        
        ttk.Label(dest_frame, text="Save To:").pack(side=tk.LEFT)
        self.dest_var = tk.StringVar(value=self.download_info['dest_path'])
        self.dest_entry = ttk.Entry(dest_frame, textvariable=self.dest_var, width=50)
        self.dest_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        
        self.browse_btn = ttk.Button(dest_frame, text="Browse...", command=self.browse_file)
        self.browse_btn.pack(side=tk.LEFT)
        
        # Status
        status_frame = ttk.LabelFrame(self.root, text="Status", padding="10")
        status_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.status_var = tk.StringVar(value="Waiting to start...")
        ttk.Label(status_frame, textvariable=self.status_var, font=("Arial", 10)).pack(anchor=tk.W)
        
        # Progress Bar
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(status_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=5)
        
        # Stats
        stats_frame = ttk.Frame(status_frame)
        stats_frame.pack(fill=tk.X)
        
        self.speed_var = tk.StringVar(value="Speed: 0.0 MB/s")
        self.cwnd_var = tk.StringVar(value="Threads: 1")
        self.segments_var = tk.StringVar(value="Segments: 1")
        
        ttk.Label(stats_frame, textvariable=self.speed_var, width=20).pack(side=tk.LEFT)
        ttk.Label(stats_frame, textvariable=self.cwnd_var, width=15).pack(side=tk.LEFT)
        ttk.Label(stats_frame, textvariable=self.segments_var, width=15).pack(side=tk.LEFT)

        # Log View
        log_label_frame = ttk.LabelFrame(self.root, text="Download Log", padding="5")
        log_label_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.log_text = tk.Text(log_label_frame, height=8, width=70, font=("Consolas", 8))
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(log_label_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        
        # Controls
        ctrl_frame = ttk.Frame(self.root, padding="10")
        ctrl_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        self.start_btn = ttk.Button(ctrl_frame, text="Start Download", command=self.start_download)
        self.start_btn.pack(side=tk.RIGHT, padx=5)
        
        self.cancel_btn = ttk.Button(ctrl_frame, text="Cancel", command=self.cancel_download)
        self.cancel_btn.pack(side=tk.RIGHT, padx=5)
        
    def browse_file(self):
        filename = filedialog.asksaveasfilename(
            initialfile=Path(self.dest_var.get()).name,
            initialdir=Path(self.dest_var.get()).parent
        )
        if filename:
            self.dest_var.set(filename)

    def start_download(self):
        self.start_btn.config(state=tk.DISABLED)
        self.browse_btn.config(state=tk.DISABLED)
        self.dest_entry.config(state=tk.DISABLED)
        self.status_var.set("Downloading...")
        self.progress_bar.config(mode='indeterminate')
        self.progress_bar.start(10)
        
        # Start download thread
        self.download_thread = threading.Thread(target=self.run_download)
        self.download_thread.daemon = True
        self.download_thread.start()
        
        # Start monitor thread
        self.monitor_thread = threading.Thread(target=self.monitor_logs)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def run_download(self):
        try:
            self.download_result = adaptive_download(
                self.client, 
                self.download_info['url'], 
                self.dest_var.get(), 
                self.download_info['cookies'], 
                self.download_info['user_agent'], 
                MAX_CWND, 
                size=self.download_info['size']
            )
            self.root.after(0, self.on_download_finished)
        except Exception as e:
            self.download_result = {"status": "error", "message": str(e)}
            self.root.after(0, self.on_download_finished)

    def monitor_logs(self):
        """Tail the log file to update UI."""
        if not self.log_file.exists():
            return

        with open(self.log_file, "r", encoding="utf-8") as f:
            # Move to end
            f.seek(0, 2)
            
            while not self.stop_event.is_set():
                line = f.readline()
                if line:
                    self.root.after(0, lambda l=line: self.append_log(l))
                    self.parse_log_line(line)
                else:
                    time.sleep(0.1)

    def append_log(self, line):
        self.log_text.insert(tk.END, line)
        self.log_text.see(tk.END)

    def parse_log_line(self, line):
        """Parse log lines to update stats."""
        # Simple parsing for demo purposes
        # Need to match specific patterns from logger.go
        if "CWND Change:" in line:
            import re
            m = re.search(r"Change: \d+ -> (\d+)", line)
            if m:
                self.cwnd_var.set(f"Threads: {m.group(1)}")
        
        if "Splitting Worker" in line:
            import re
            m = re.search(r"Worker (\d+)", line) # Assuming ID helps count? No, just detect split
            # Just increment or parse total segments from split log if available?
            # logger.go: "Splitting Worker %d ... New Worker %d"
            m = re.search(r"New Worker (\d+)", line)
            if m:
                self.segments_var.set(f"Segments: {m.group(1)}")

        if "Segment Complete" in line:
             # Stop indeterminate bar if it was running, maybe switch to determinate if we knew progress
             pass

    def on_download_finished(self):
        self.stop_event.set()
        self.progress_bar.stop()
        self.progress_bar.config(mode='determinate', value=100)
        
        res = self.download_result
        if res and res.get("status") == "success":
            self.status_var.set("Download Complete!")
            self.start_btn.config(text="Finish & Close", state=tk.NORMAL, command=self.finish_and_close)
            self.cancel_btn.config(state=tk.DISABLED)
            
            # Update final stats
            self.speed_var.set(f"Avg Speed: {res.get('bytes_total', 0) / (res.get('time_elapsed', 1) * 1024 * 1024):.2f} MB/s")
            self.cwnd_var.set(f"Threads: {res.get('final_cwnd', 1)}")
            self.segments_var.set(f"Segments: {res.get('segment_count', 1)}")
            
            messagebox.showinfo("Success", f"Download finished successfully!\nSaved to: {self.dest_var.get()}")
            
        else:
            self.status_var.set("Download Failed")
            err_msg = res.get("error") if res else "Unknown error"
            messagebox.showerror("Error", f"Download failed: {err_msg}")
            self.start_btn.config(state=tk.NORMAL)
            self.browse_btn.config(state=tk.NORMAL)
            self.dest_entry.config(state=tk.NORMAL)

    def cancel_download(self):
        if messagebox.askyesno("Cancel", "Are you sure you want to cancel?"):
            self.stop_event.set()
            # We can't easily kill the Go process via MCP client without stopping the whole server
            # But the client library might support cancellation if context was used?
            # host.py doesn't expose context cancellation easily yet.
            # Best we can do is close window and return 'cancelled' to Chrome.
            self.on_complete_callback({"status": "error", "message": "User cancelled"})
            self.root.destroy()

    def finish_and_close(self):
        """Send success to Chrome and close."""
        result = self.download_result
        final_res = {
            "status": "success",
            "action": "downloaded",
            "file_path": self.dest_var.get(),
            "file_size": result.get("bytes_total", 0),
            "segments": result.get("segment_count", 0),
            "final_cwnd": result.get("final_cwnd", 1),
            "time_elapsed": result.get("time_elapsed", 0)
        }
        self.on_complete_callback(final_res)
        self.root.destroy()

    def run(self):
        self.root.mainloop()




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
        
        # Use Chrome's totalBytes as fallback
        if file_size < 1024 * 1024 and chrome_total_bytes > 1024 * 1024:
            log(f"HEAD returned {file_size} bytes, using Chrome's totalBytes: {chrome_total_bytes}")
            file_size = chrome_total_bytes
        
        size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
        
        log(f"Detected: size={size_mb:.1f}MB, accept_ranges={accept_ranges}, filename={filename}")
        
        # Decision logic: if small file, skip
        if size_mb < SIZE_THRESHOLD_MB or not accept_ranges:
            return {
                "status": "skip",
                "action": "continue_native",
                "reason": f"Size {size_mb:.1f}MB < {SIZE_THRESHOLD_MB}MB or no range support",
                "file_size": file_size,
                "accept_ranges": accept_ranges
            }

        # Use UI for large files
        dest_path = str(DOWNLOAD_DIR / filename) # Default path
        
        # Determine strict default path (same logic as before)
        if os.sep in filename or '/' in filename:
            potential_path = Path(filename)
            if potential_path.is_absolute():
                dest_path = str(potential_path)
            else:
                dest_path = str(DOWNLOAD_DIR / filename)
        
        # Prepare info for UI
        download_info = {
            "url": url,
            "dest_path": dest_path,
            "cookies": cookies,
            "user_agent": user_agent,
            "size": file_size
        }
        
        # Run UI
        ui_result = {}
        
        def on_ui_complete(result):
            nonlocal ui_result
            ui_result = result
            
        app = DownloadManagerUI(client, download_info, on_ui_complete)
        app.run()
        
        if ui_result:
            return ui_result
        else:
            return {"status": "error", "message": "UI closed without result"}
    
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
