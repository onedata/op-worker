export default function snakeToCamel(text, snakeSeparator) {
  let sep = snakeSeparator || '-';
  let re = new RegExp(`(\\${sep}\\w)`, 'g');
  return text.replace(re, (m) => { return m[1].toUpperCase(); });
}
