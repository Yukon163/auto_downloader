# Intelligent Download Agent

A local intelligent download system that intercepts browser download requests and uses multi-threaded downloads for large files.

## Architecture

```
Chrome Extension  →  Python Agent (Native Messaging)  →  Go MCP Server
   (Client)              (Controller)                      (Worker)
```

## Quick Setup

### 1. Build Go Downloader

```bash
cd G-Downloader
go build -o go-downloader.exe .
```

### 2. Install Chrome Extension

1. Open Chrome and go to `chrome://extensions`
2. Enable "Developer mode" (top right toggle)
3. Click "Load unpacked" and select the `chorme_donlodIntercepter` folder
4. Copy the **Extension ID** shown under the extension

### 3. Configure Native Messaging

1. Edit `local_agent/com.autodownloader.agent.json`
2. Replace `EXTENSION_ID_PLACEHOLDER` with your actual extension ID
3. Run `local_agent/install_host.bat` as Administrator

### 4. Test

Download any file > 50MB. The extension will:
- Intercept the download
- Send it to the Python agent
- Use multi-threaded Go downloader if file supports range requests
- Show notification on completion

## Components

| Component | Path | Description |
|-----------|------|-------------|
| Go MCP Server | `G-Downloader/` | Multi-threaded downloader exposing `detect_resource` and `concurrent_download` tools |
| Python Agent | `local_agent/` | Native messaging host with download decision logic |
| Chrome Extension | `chorme_donlodIntercepter/` | Download interceptor using Manifest V3 |

## Configuration

Edit `local_agent/host.py` to change:
- `SIZE_THRESHOLD_MB` - minimum file size for multi-threaded download (default: 50MB)
- `MAX_THREADS` - maximum concurrent connections (default: 16)
- `CHUNK_SIZE_MB` - size per thread calculation (default: 10MB)

## Requirements

- Go 1.21+
- Python 3.8+
- Chrome/Chromium browser
