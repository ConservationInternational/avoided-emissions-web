(function () {
    let activeRequest = null;
    let bound = false;
    let fileInput = null;

    function emitPayload(payload) {
        const payloadInput = document.getElementById("site-upload-stream-payload");
        const payloadJson = JSON.stringify(payload);

        if (
            window.dash_clientside &&
            typeof window.dash_clientside.set_props === "function"
        ) {
            try {
                window.dash_clientside.set_props("site-upload-stream-payload", {
                    value: payloadJson,
                });
                return true;
            } catch (_error) {
                // Fall through to the DOM event path below.
            }
        }

        if (!payloadInput) {
            return false;
        }

        const setter = Object.getOwnPropertyDescriptor(
            window.HTMLInputElement.prototype,
            "value",
        )?.set;
        if (setter) {
            setter.call(payloadInput, payloadJson);
        } else {
            payloadInput.value = payloadJson;
        }

        payloadInput.dispatchEvent(new Event("input", { bubbles: true }));
        payloadInput.dispatchEvent(new Event("change", { bubbles: true }));
        return true;
    }

    function setStatus(message) {
        const status = document.getElementById("upload-status");
        if (status) {
            status.textContent = message;
        }
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

    function uploadFileWithProgress(file, button) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            let lastAnnouncedProgress = -10;
            activeRequest = xhr;
            xhr.open("POST", "/api/site-upload/stream-preview", true);
            xhr.withCredentials = true;

            xhr.upload.onprogress = (event) => {
                if (!event.lengthComputable) {
                    return;
                }
                const pct = event.total > 0
                    ? Math.min(100, Math.round((event.loaded / event.total) * 100))
                    : 0;
                button.textContent = `Uploading... ${pct}% (${formatBytes(event.loaded)} / ${formatBytes(event.total)})`;
                if (pct >= lastAnnouncedProgress + 10 || pct === 100) {
                    setStatus(`Uploading ${file.name}: ${pct}% transferred.`);
                    lastAnnouncedProgress = pct;
                }
            };

            xhr.upload.onload = () => {
                button.textContent = "Processing preview...";
                setStatus("Upload complete. Preparing the mapping preview.");
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

    async function handleFile(file) {
        const button = document.getElementById("upload-sites-stream-btn");
        if (!button || !file || activeRequest) {
            return;
        }

        const originalText = button.textContent;
        button.disabled = true;
        button.textContent = "Uploading...";
        const cancelButton = document.getElementById("upload-sites-stream-cancel-btn");
        if (cancelButton) {
            cancelButton.disabled = false;
            cancelButton.style.display = "inline-block";
        }
        setStatus(`Starting upload of ${file.name}.`);

        try {
            const response = await uploadFileWithProgress(file, button);
            const body = response.body || {
                ok: false,
                errors: ["Unexpected server response while uploading file."],
            };
            const emitted = emitPayload({
                ts: Date.now(),
                status: response.ok && body.ok ? "ok" : "error",
                filename: file.name,
                response: body,
                http_status: response.status,
            });
            if (!emitted) {
                setStatus("Upload finished, but the page needs to be refreshed.");
            } else if (response.ok && body.ok) {
                setStatus("Mapping preview ready.");
            } else {
                const errors = Array.isArray(body.errors)
                    ? body.errors.filter(message => typeof message === "string")
                    : [];
                setStatus(`Upload preview failed. ${errors.join(" ") || "Please try again."}`);
            }
        } catch (error) {
            const cancelled = error && error.message === "Upload was cancelled.";
            emitPayload({
                ts: Date.now(),
                status: "error",
                filename: file.name,
                response: {
                    ok: false,
                    errors: [cancelled ? "Upload cancelled." : "Upload failed. Please try again."],
                },
                error: String(error),
            });
            setStatus(cancelled ? "Upload cancelled." : "Upload failed. Please try again.");
        } finally {
            activeRequest = null;
            if (fileInput) {
                fileInput.value = "";
            }
            const currentButton = document.getElementById("upload-sites-stream-btn");
            if (currentButton) {
                currentButton.disabled = false;
                currentButton.textContent = originalText;
            }
            const cancelButton = document.getElementById("upload-sites-stream-cancel-btn");
            if (cancelButton) {
                cancelButton.disabled = true;
                cancelButton.style.display = "none";
            }
        }
    }

    function bindUploadControls() {
        if (bound) {
            return;
        }
        bound = true;

        fileInput = document.createElement("input");
        fileInput.type = "file";
        fileInput.id = "upload-sites-stream-input";
        fileInput.accept = ".geojson,.json,.gpkg,.zip,.tar.gz,.tgz";
        fileInput.hidden = true;
        fileInput.setAttribute("aria-hidden", "true");
        document.body.appendChild(fileInput);

        document.addEventListener("click", (event) => {
            const target = event.target instanceof Element ? event.target : null;
            if (!target) {
                return;
            }
            const uploadButton = target.closest("#upload-sites-stream-btn");
            if (uploadButton) {
                if (!activeRequest) {
                    fileInput.click();
                }
                return;
            }

            const cancelButton = target.closest("#upload-sites-stream-cancel-btn");
            if (cancelButton && activeRequest) {
                activeRequest.abort();
            }
        });

        fileInput.addEventListener("change", () => {
            const file = fileInput.files && fileInput.files[0];
            if (file) {
                handleFile(file);
            }
        });
    }

    function init() {
        bindUploadControls();
        const cancelButton = document.getElementById("upload-sites-stream-cancel-btn");
        if (cancelButton) {
            cancelButton.disabled = !activeRequest;
        }
    }

    document.addEventListener("DOMContentLoaded", init);
    const observer = new MutationObserver(init);
    observer.observe(document.documentElement, { childList: true, subtree: true });
    init();
})();
