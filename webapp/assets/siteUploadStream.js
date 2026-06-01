(function () {
    function emitPayload(payloadInput, payload) {
        payloadInput.value = JSON.stringify(payload);
        payloadInput.dispatchEvent(new Event("input", { bubbles: true }));
        payloadInput.dispatchEvent(new Event("change", { bubbles: true }));
    }

    function bindUploadControls() {
        const button = document.getElementById("upload-sites-stream-btn");
        const fileInput = document.getElementById("upload-sites-stream-input");
        const payloadInput = document.getElementById("site-upload-stream-payload");

        if (!button || !fileInput || !payloadInput || button.dataset.streamBound === "1") {
            return;
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
            button.textContent = "Uploading...";

            try {
                const formData = new FormData();
                formData.append("file", file, file.name);

                const response = await fetch("/api/site-upload/stream-preview", {
                    method: "POST",
                    body: formData,
                    credentials: "same-origin",
                });

                let body;
                try {
                    body = await response.json();
                } catch (_error) {
                    body = {
                        ok: false,
                        errors: ["Unexpected server response while uploading file."],
                    };
                }

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
