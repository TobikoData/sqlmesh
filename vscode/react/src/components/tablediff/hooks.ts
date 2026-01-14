import { useState, useEffect } from 'react'

/**
 * Persist state in localStorage so the user's expand / collapse choices
 * survive reloads and navigation in VS Code's WebView.
 */
export function usePersistedState<T>(
  key: string,
  initial: T,
): [T, React.Dispatch<React.SetStateAction<T>>] {
  const [state, setState] = useState<T>(() => {
    try {
      const stored = localStorage.getItem(key)
      return stored ? (JSON.parse(stored) as T) : initial
    } catch {
      return initial
    }
  })

  useEffect(() => {
    try {
      localStorage.setItem(key, JSON.stringify(state))
    } catch {
      /* noop */
    }
  }, [key, state])

  return [state, setState]
}
