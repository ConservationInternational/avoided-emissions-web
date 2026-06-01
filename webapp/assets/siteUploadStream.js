(function () {
    function emitPayload(payloadInput, payload) {
        payloadInput.value = JSON.stringify(payload);
        payloadInput.dispatchEvent(new Event("input", { bubbles: true }));
        payloadInput.dispatchEvent(new Event("change", { bubbles: true }));
    }

    function formatBytes(value) {
        const bytes = Number(value) || 0;
        if (bytes < 1024) {
            return `${bytes} B`;
        }
        const units = ["KB", "MB", "GB", "TB"];
        let size = bytes / 1024;
        let unitIndex = 0;
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex += 1;
        }
        return `${size.toFixed(1)} ${units[unitIndex]}`;
    }

    function uploadFileWithProgress(file, onProgress) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open("POST", "/api/site-upload/stream-preview", true);
            xhr.withCredentials = true;

            xhr.upload.onprogress = (event) => {
                if (!event.lengthComputable || typeof onProgress !== "function") {
                    return;
                }
                onProgress(event.loaded, event.total);
            };

            xhr.onload = () => {
                let body;
                try {
                    body = xhr.responseText
                        ? JSON.parse(xhr.responseText)
                        : { ok: false, errors: ["Empty server response."] };
                } catch (_error) {
                    body = {
                        ok: false,
                        errors: ["Unexpected server response while uploading file."],
                    };
                }

                resolve({
                    status: xhr.status,
                    ok: xhr.status >= 200 && xhr.status < 300,
                    body,
                });
            };

            xhr.onerror = () => {
                reject(new Error("Network error during upload."));
            };

            xhr.onabort = () => {
                reject(new Error("Upload was cancelled."));
            };

            const formData = new FormData();
            formData.append("file", file, file.name);
            xhr.send(formData);
        });
    }

    function bindUploadControls() {
        const button = document.getElementById("upload-sites-stream-btn");
        const payloadInput = document.getElementById("site-upload-stream-payload");

        if (!button || !payloadInput || button.dataset.streamBound === "1") {
            return;
        }

        // Create the hidden file input dynamically to avoid Dash html.Input limitations.
        let fileInput = document.getElementById("upload-sites-stream-input");
        if (!fileInput) {
            fileInput = document.createElement("input");
            fileInput.type = "file";
            fileInput.id = "upload-sites-stream-input";
            fileInput.accept = ".geojson,.json,.gpkg,.zip,.tar.gz,.tgz";
            fileInput.style.display = "none";
            document.body.appendChild(fileInput);
        }

        button.dataset.streamBound = "1";
        button.addEventListener("click", () => {
            fileInput.click();
        });

        fileInput.addEventListener("change", async () => {
            const file = fileInput.files && fileInput.files[0];
            if (!file) {
                return;
            }

            const originalText = button.textContent;
            button.disabled = true;
            button.textContent = "Uploading... 0%";

            try {
                const response = await uploadFileWithProgress(
                    file,
                    (loaded, total) => {
                        const pct = total > 0 ? Math.min(100, Math.round((loaded / total) * 100)) : 0;
                        button.textContent = `Uploading... ${pct}% (${formatBytes(loaded)} / ${formatBytes(total)})`;
                    },
                );

                // Upload bytes have reached the server; server may still be
                // parsing the file and generating a preview.
                button.textContent = "Processing preview...";

                const body = response.body || {
                    ok: false,
                    errors: ["Unexpected server response while uploading file."],
                };

                emitPayload(payloadInput, {
                    ts: Date.now(),
                    status: response.ok && body.ok ? "ok" : "error",
                    filename: file.name,
                    response: body,
                    http_status: response.status,
                });
            } catch (error) {
                emitPayload(payloadInput, {
                    ts: Date.now(),
                    status: "error",
                    filename: file.name,
                    response: {
                        ok: false,
                        errors: ["Upload failed. Please try again."],
                    },
                    error: String(error),
                });
            } finally {
                fileInput.value = "";
                button.disabled = false;
                button.textContent = originalText;
            }
        });
    }

    function init() {
        bindUploadControls();
    }

    document.addEventListener("DOMContentLoaded", init);
    const observer = new MutationObserver(init);
    observer.observe(document.documentElement, { childList: true, subtree: true });
})();
