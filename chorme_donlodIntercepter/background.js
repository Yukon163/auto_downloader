/**
 * Auto Downloader - Chrome Extension Background Service Worker
 * Intercepts downloads and delegates large files to native messaging host
 */

const NATIVE_HOST_NAME = "com.autodownloader.agent";
const SIZE_THRESHOLD_BYTES = 50 * 1024 * 1024; // 50MB - but we check in Python too

// Track downloads we're handling
const handledDownloads = new Set();
const pendingDownloads = new Map();

/**
 * Connect to native messaging host and send a download request
 */
async function sendToNativeHost(downloadInfo) {
    return new Promise((resolve, reject) => {
        try {
            const port = chrome.runtime.connectNative(NATIVE_HOST_NAME);

            port.onMessage.addListener((response) => {
                console.log("Native host response:", response);
                port.disconnect();
                resolve(response);
            });

            port.onDisconnect.addListener(() => {
                const error = chrome.runtime.lastError;
                if (error) {
                    console.error("Native host disconnected with error:", error.message);
                    reject(new Error(error.message));
                }
            });

            // Send the download request
            port.postMessage(downloadInfo);

        } catch (error) {
            console.error("Failed to connect to native host:", error);
            reject(error);
        }
    });
}

/**
 * Show a notification to the user
 */
function showNotification(title, message, isError = false) {
    chrome.notifications.create({
        type: "basic",
        iconUrl: "icons/icon128.png",
        title: title,
        message: message,
        priority: isError ? 2 : 1
    });
}

/**
 * Get cookies for a URL
 */
async function getCookiesForUrl(url) {
    try {
        const cookies = await chrome.cookies.getAll({ url });
        return cookies.map(c => `${c.name}=${c.value}`).join("; ");
    } catch (e) {
        console.warn("Could not get cookies:", e);
        return "";
    }
}

/**
 * Handle download creation event
 */
chrome.downloads.onCreated.addListener(async (downloadItem) => {
    const downloadId = downloadItem.id;
    const url = downloadItem.finalUrl || downloadItem.url;

    // Skip if we already handled this or if it's a data URI
    if (handledDownloads.has(downloadId) || url.startsWith("data:") || url.startsWith("blob:")) {
        return;
    }

    console.log(`Download intercepted: ${url}`);

    // Mark as handled to prevent re-processing
    handledDownloads.add(downloadId);

    // Pause the download immediately
    try {
        await chrome.downloads.pause(downloadId);
    } catch (e) {
        console.warn("Could not pause download:", e);
    }

    // Store pending download info
    pendingDownloads.set(downloadId, {
        url: url,
        filename: downloadItem.filename,
        fileSize: downloadItem.fileSize,
        startTime: Date.now()
    });

    try {
        // Get cookies for the URL
        const cookies = await getCookiesForUrl(url);

        // Send to native host
        const response = await sendToNativeHost({
            url: url,
            cookies: cookies,
            userAgent: navigator.userAgent,
            filename: downloadItem.filename || ""
        });

        if (response.status === "success" && response.action === "downloaded") {
            // Download completed by our agent
            console.log(`Download completed by agent: ${response.file_path}`);

            // Cancel the Chrome download since we handled it
            await chrome.downloads.cancel(downloadId);

            const sizeMB = (response.file_size / (1024 * 1024)).toFixed(1);
            const timeSeconds = response.time_elapsed.toFixed(1);

            showNotification(
                "Download Complete",
                `${response.file_path}\n${sizeMB}MB in ${timeSeconds}s (${response.threads_used} threads)`
            );

        } else if (response.action === "continue_native") {
            // Let Chrome handle it
            console.log(`Letting Chrome handle download: ${response.reason || response.message}`);

            try {
                await chrome.downloads.resume(downloadId);
            } catch (e) {
                console.warn("Could not resume download:", e);
            }

        } else {
            // Unknown response, resume native download
            console.warn("Unknown response from native host:", response);
            await chrome.downloads.resume(downloadId);
        }

    } catch (error) {
        console.error("Error handling download:", error);

        // On error, let Chrome handle the download
        try {
            await chrome.downloads.resume(downloadId);
        } catch (e) {
            console.warn("Could not resume download after error:", e);
        }

        showNotification(
            "Auto Downloader Error",
            `Falling back to Chrome download: ${error.message}`,
            true
        );
    }

    // Cleanup
    pendingDownloads.delete(downloadId);

    // Remove from handled set after a delay
    setTimeout(() => {
        handledDownloads.delete(downloadId);
    }, 5000);
});

/**
 * Handle download completion/cancellation
 */
chrome.downloads.onChanged.addListener((delta) => {
    if (delta.state) {
        const downloadId = delta.id;

        if (delta.state.current === "complete" || delta.state.current === "interrupted") {
            pendingDownloads.delete(downloadId);
            handledDownloads.delete(downloadId);
        }
    }
});

/**
 * Extension icon click - show status
 */
chrome.action.onClicked.addListener(() => {
    const pendingCount = pendingDownloads.size;

    if (pendingCount === 0) {
        showNotification("Auto Downloader", "Ready to intercept large downloads (>50MB)");
    } else {
        showNotification("Auto Downloader", `${pendingCount} download(s) in progress`);
    }
});

// Log when extension loads
console.log("Auto Downloader extension loaded");
