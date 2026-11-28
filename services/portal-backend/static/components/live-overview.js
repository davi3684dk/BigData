import { units } from "../format.js";

export default {
  props: ["live", "failed", "checked"],
  methods: { units },
  computed: {
    status() {
      if (this.failed) return "Live update unavailable · retrying";
      if (this.live?.enabled === false) return "History-only mode";
      return this.checked ? "Checked " + this.checked + " · every 10 seconds" : "Checking for updates…";
    },
  },
  template: `
    <section aria-labelledby="live-title">
      <div class="heading">
        <h2 id="live-title">Live overview</h2>
        <small role="status">{{ status }}</small>
      </div>
      <div class="cards">
        <article class="weather">
          <h3>London weather</h3>
          <p class="metric">{{ units(live?.weather?.mean_temp, " °C") }}</p>
          <p v-if="live?.weather">{{ live.weather.date }}
            <span v-if="live.weather.precipitation != null"> · {{ units(live.weather.precipitation, " mm rain") }}</span>
          </p>
          <p v-else-if="live?.enabled === false">Live weather is off.</p>
          <p v-else-if="failed">Observation unavailable.</p>
          <p v-else>Waiting for a weather observation.</p>
        </article>
        <article class="forecast">
          <h3>Latest order prediction</h3>
          <p class="metric">{{ units(live?.prediction?.predicted_orders, " orders") }}</p>
          <p v-if="live?.prediction">Restaurant {{ live.prediction.restaurant_id }} · {{ live.prediction.date }}</p>
          <p v-else-if="live?.enabled === false">Live predictions are off.</p>
          <p v-else-if="failed">Prediction unavailable.</p>
          <p v-else>Waiting for an order prediction.</p>
        </article>
      </div>
    </section>`,
};
