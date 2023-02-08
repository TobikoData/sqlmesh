export function isArrayNotEmpty(value: unknown): boolean {
  return Array.isArray(value) && value.length > 0;
}

export function isArrayEmpty(value: unknown): boolean {
  return Array.isArray(value) && value.length === 0;
}

export function isObjectEmpty(value: unknown): boolean {
  return isObject(value) && isArrayEmpty(Object.keys(value as object));
}

export function isObject(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    value.constructor === Object
  );
}

export function isNil(value: unknown): boolean {
  return value == null;
}

export function isNotNil(value: unknown): boolean {
  return value != null;
}

export async function delay(time: number = 1000): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, time)
  })
}

export function isDate(value: unknown): boolean {
  return value instanceof Date;
}

export function toDate(str: string | number): Date | null {
  try {
    const date = new Date(str);

    return isNaN(date.getTime())
      ? null
      : date;
  } catch {
    return null;
  }
}

export function toDateFormat(date: Date | null, format: string = 'yyyy-mm-dd'): string {
  if (date == null) return '';
  
  const year = date.getFullYear();
  const month = date.getMonth() + 1
  const day = date.getDate()
  const hour = date.getHours()
  const minute = date.getMinutes()
  const second = date.getSeconds()

  if (format === 'yyyy-mm-dd') return `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
  if (format === 'yyyy-mm-dd hh-mm-ss') return `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')} ${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}:${second.toString().padStart(2, '0')}`;

  return date.toString();
}


