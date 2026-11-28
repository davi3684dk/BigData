import { money, units } from "../format.js";

export default {
  props: ["live"],
  data: () => ({ preferredRestaurant: "" }),
  computed: {
    outlook() { return this.live?.outlook; },
    predictions() { return this.outlook?.predictions ?? []; },
    restaurants() {
      return [...new Set(this.predictions.map(row => row.restaurant_id))].sort();
    },
    selected() {
      return this.restaurants.includes(this.preferredRestaurant) ? this.preferredRestaurant : this.restaurants[0];
    },
    days() {
      const byDay = new Map(this.predictions
        .filter(row => row.restaurant_id === this.selected)
        .map(row => [row.date, row]));
      return Object.keys(this.outlook?.days ?? {}).sort().map(date => ({
        date, weather: this.outlook.days[date], prediction: byDay.get(date),
      }));
    },
    scoredDays() { return this.days.filter(day => day.prediction).length; },
  },
  methods: {
    money, units,
    total(field) {
      if (this.days.length !== 7 || this.days.some(day => day.prediction?.[field] == null)) return null;
      return this.days.reduce((sum, day) => sum + day.prediction[field], 0);
    },
  },
  template: `
    <section class="history" aria-labelledby="outlook-title">
      <div class="heading">
        <div>
          <h2 id="outlook-title">Seven-day outlook</h2>
          <p v-if="outlook?.issued_at">London forecast issued {{ outlook.issued_at.replace("T", " ") }}</p>
          <p v-else-if="live?.enabled === false">The weekly outlook requires live forecast data.</p>
          <p v-else>Waiting for the London weather forecast.</p>
        </div>
        <label>Restaurant
          <select aria-label="Restaurant" :value="selected" @change="preferredRestaurant = $event.target.value" :disabled="!restaurants.length">
            <option v-for="id in restaurants" :key="id" :value="id">{{ id }}</option>
          </select>
        </label>
      </div>
      <div class="cards">
        <article class="forecast"><h3>Weekly orders</h3><p class="metric">{{ units(total("predicted_orders")) }}</p></article>
        <article class="forecast"><h3>Weekly revenue</h3><p class="metric">{{ money(total("predicted_revenue")) }}</p></article>
      </div>
      <p class="coverage" role="status"><template v-if="days.length">Latest sales predictions available for {{ scoredDays }} of {{ days.length }} forecast days.</template></p>
      <div class="table-wrap">
        <table aria-label="Daily weather and sales forecast">
          <thead><tr><th scope="col">Forecast day</th><th scope="col">Mean temperature</th><th scope="col">Rain</th><th scope="col">Orders</th><th scope="col">Revenue</th></tr></thead>
          <tbody>
            <tr v-for="day in days" :key="day.date">
              <th scope="row">{{ day.date }}</th>
              <td>{{ units(day.weather.mean_temp, " °C") }}</td>
              <td>{{ units(day.weather.precipitation, " mm") }}</td>
              <td>{{ units(day.prediction?.predicted_orders) }}</td>
              <td>{{ money(day.prediction?.predicted_revenue) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>`,
};
