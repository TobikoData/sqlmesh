export function isArrayNotEmpty(value: unknown): boolean {
  return Array.isArray(value) && value.length > 0;
}

export function isArrayEmpty(value: unknown): boolean {
  return Array.isArray(value) && value.length === 0;
}

export async function delay(time: number = 1000): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, time)
  })
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
