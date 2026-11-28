import { createApp } from "./vendor/vue.esm-browser.prod.js";
import { usePolling } from "./polling.js";
import LiveOverview from "./components/live-overview.js";
import WeeklyOutlook from "./components/weekly-outlook.js";
import DailyHistory from "./components/daily-history.js";

createApp({
  components: { LiveOverview, WeeklyOutlook, DailyHistory },
  setup() {
    const { data: live, failed, checked } = usePolling(() => "/api/live", 10000, {
      stopWhen: snapshot => snapshot.enabled === false,
    });
    return { live, failed, checked };
  },
}).mount("#dashboard");
