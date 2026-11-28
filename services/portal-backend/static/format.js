const numbers = new Intl.NumberFormat("en-GB");
const pounds = new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" });

export function units(value, suffix = "") {
  return value == null ? "—" : numbers.format(value) + suffix;
}

export function money(value) {
  return value == null ? "—" : pounds.format(value);
}
