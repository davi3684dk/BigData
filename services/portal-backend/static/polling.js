import { ref, watch } from "./vendor/vue.esm-browser.prod.js";

// A URL change (including a retry) cancels the previous request and its timer.
export function usePolling(url, interval, { stopWhen = () => false } = {}) {
  const data = ref(null);
  const failed = ref(false);
  const checked = ref("");
  const attempt = ref(0);

  watch([url, attempt], ([path], previous, onCleanup) => {
    let timer;
    const controller = new AbortController();
    onCleanup(() => {
      controller.abort();
      clearTimeout(timer);
    });
    data.value = null;
    checked.value = "";
    failed.value = false;
    if (!path) return;

    async function refresh() {
      const timeout = AbortSignal.timeout(8000);
      try {
        const response = await fetch(path, {
          cache: "no-store",
          signal: AbortSignal.any([controller.signal, timeout]),
        });
        if (!response.ok) throw new Error("Request failed");
        const snapshot = await response.json();
        if (controller.signal.aborted) return;
        data.value = snapshot;
        checked.value = new Date().toLocaleTimeString("en-GB");
        failed.value = false;
        if (stopWhen(snapshot)) return;
      } catch {
        if (controller.signal.aborted) return;
        failed.value = true;
      }
      timer = setTimeout(refresh, interval);
    }
    refresh();
  }, { immediate: true });

  return { data, failed, checked, retry: () => attempt.value++ };
}
