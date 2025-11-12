/**
 * A result is a value that can be either an ok or an error
 */
export type Result<T, E> = { ok: true; value: T } | { ok: false; error: E }

/**
 * returns true if the result is an error
 */
export const isErr = <T, E>(
  result: Result<T, E>,
): result is { ok: false; error: E } => {
  return !result.ok
}

/**
 * returns an ok version `Result<T, E>` from a value `T`
 */
export const ok = <T>(value: T): { ok: true; value: T } => {
  return { ok: true, value }
}

/**
 * returns an error version `Result<T, E>` from an error `E`
 */
export const err = <E>(error: E): { ok: false; error: E } => {
  return { ok: false, error }
}
