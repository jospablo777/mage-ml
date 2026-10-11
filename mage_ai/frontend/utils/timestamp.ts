import moment from 'moment';

/**
 * A timestamp as the rest of the app shows it: UTC with its zone by default, local time
 * when the workspace setting asks for it. Takes an ISO string or milliseconds.
 */
export function displayTimestamp(value: string | number, displayLocal: boolean = false): string {
  const parsed = typeof value === 'number' ? moment.utc(value) : moment.utc(value, moment.ISO_8601);
  if (!parsed.isValid()) {
    return String(value);
  }

  return displayLocal
    ? parsed.local().format('YYYY-MM-DD HH:mm:ss')
    : `${parsed.format('YYYY-MM-DD HH:mm:ss')} UTC`;
}
