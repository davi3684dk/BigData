import { ref } from "../vendor/vue.esm-browser.prod.js";
import { usePolling } from "../polling.js";
import { units } from "../format.js";

export default {
  setup() {
    const day = ref("2018-01-02");
    const { data: history, failed, retry } = usePolling(
      () => day.value && "/api/history/" + encodeURIComponent(day.value), 30000,
    );
    return { day, history, failed, retry };
  },
  computed: {
    maximum() {
      return (this.history?.restaurants ?? []).reduce(
        (max, row) => Math.max(max, row.actual_orders ?? 0, row.predicted_orders ?? 0), 1,
      );
    },
    status() {
      if (!this.day) return "Choose an order date.";
      if (this.failed) return "Could not load history for this date.";
      if (!this.history) return "Loading history…";
      return this.history.restaurants.length ? "" : "No orders or predictions recorded for this date.";
    },
  },
  methods: {
    units,
    errorPercent(row) {
      if (!row.actual_orders || row.difference == null) return "—";
      return units(100 * Math.abs(row.difference) / row.actual_orders, "%");
    },
  },
  template: `
    <section class="history" aria-labelledby="history-title">
      <div class="heading">
        <div><h2 id="history-title">Daily comparison</h2><p>Actual and predicted orders for each restaurant.</p></div>
        <label>Order date<input type="date" v-model="day" required></label>
      </div>
      <p v-if="history" class="observations">{{ history.weather
        ? "Daily mean temperature  " + units(history.weather.mean_temp, " °C")
        : "No weather observation for this date." }}</p>
      <div class="table-wrap">
        <table aria-label="Daily order comparison">
          <thead><tr><th scope="col">Restaurant</th><th scope="col">Actual orders</th><th scope="col">Predicted orders</th><th scope="col">Difference</th><th scope="col" title="Absolute difference divided by actual orders">Absolute error (%)</th></tr></thead>
          <tbody>
            <tr v-for="row in history?.restaurants" :key="row.restaurant_id">
              <th scope="row">{{ row.restaurant_id }}</th>
              <td v-for="field in ['actual_orders', 'predicted_orders']" :key="field">
                {{ units(row[field]) }}
                <span v-if="row[field] != null" class="bar" :class="{ predicted: field === 'predicted_orders' }" aria-hidden="true">
                  <i :style="{ width: (100 * row[field] / maximum) + '%' }"></i>
                </span>
              </td>
              <td>{{ row.difference > 0 ? "+" : "" }}{{ units(row.difference) }}</td>
              <td>{{ errorPercent(row) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p id="history-status" role="status">{{ status }}</p>
      <button v-if="failed" @click="retry">Retry history</button>
    </section>`,
};
