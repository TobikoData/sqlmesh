type SemVer = [number, number, number]

/**
 * Check if a is greater than or equal to b.
 *
 * @param a - The first version.
 * @param b - The second version.
 * @returns True if a is greater than b, false otherwise.
 */
export function isSemVerGreaterThanOrEqual(a: SemVer, b: SemVer): boolean {
  if (a[0] > b[0]) {
    return true
  }
  if (a[0] < b[0]) {
    return false
  }
  if (a[1] > b[1]) {
    return true
  }
  if (a[1] < b[1]) {
    return false
  }
  return a[2] >= b[2]
}
