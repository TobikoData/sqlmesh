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
