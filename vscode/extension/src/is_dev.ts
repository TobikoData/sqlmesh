/**
 * Determines if the extension is running in production mode.
 */
export function isProduction(): boolean {
  return process.env.NODE_ENV === 'production'
}
