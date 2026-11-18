(function () {
    "use strict";

    let restoringLocation = null;
    let lastOffset = null;
    let interrupted = false;
    let article = null;
    let scheduled = false;
    const resizeObserver = new ResizeObserver(restoreDocumentationAnchor);

    function restoreDocumentationAnchor() {
        const path = window.location.pathname;
        if (path !== "/docs" && !path.startsWith("/docs/")) {
            return;
        }
        const location = path + window.location.hash;
        if (!window.location.hash) {
            return;
        }
        if (restoringLocation !== location) {
            restoringLocation = location;
            lastOffset = null;
            interrupted = false;
        }
        if (interrupted || scheduled) {
            return;
        }
        let anchor;
        try {
            anchor = decodeURIComponent(window.location.hash.slice(1));
        } catch (error) {
            if (!(error instanceof URIError)) {
                throw error;
            }
            console.warn("[Documentation] Invalid anchor encoding", error);
            interrupted = true;
            return;
        }
        const content = document.getElementById("documentation-content");
        if (content && article !== content) {
            resizeObserver.disconnect();
            resizeObserver.observe(content);
            article = content;
        }
        const target = document.getElementById(anchor);
        if (target && content?.contains(target)) {
            scheduled = true;
            requestAnimationFrame(function () {
                scheduled = false;
                if (interrupted || restoringLocation !== location) {
                    return;
                }
                const offset = target.getBoundingClientRect().top + window.scrollY;
                if (offset !== lastOffset) {
                    // Markdown and equations finish laying out after Dash mounts headings.
                    target.scrollIntoView();
                    if (lastOffset === null) {
                        target.focus({ preventScroll: true });
                    }
                    lastOffset = offset;
                }
            });
        }
    }

    new MutationObserver(restoreDocumentationAnchor).observe(document.documentElement, {
        childList: true,
        subtree: true,
    });
    window.addEventListener("hashchange", restoreDocumentationAnchor);
    window.addEventListener("popstate", function () {
        restoringLocation = null;
        restoreDocumentationAnchor();
    });
    for (const event of ["wheel", "touchstart", "pointerdown", "keydown"]) {
        window.addEventListener(event, function () { interrupted = true; }, { passive: true });
    }
    restoreDocumentationAnchor();
})();
