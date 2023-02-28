import { useCallback } from 'react'

export default function setFocus<
  TElement extends HTMLElement = HTMLInputElement,
>(timeout: number = 0): (el: TElement) => void {
  return useCallback(
    (el: TElement) => {
      setTimeout(() => {
        el?.focus()
      }, timeout)
    },
    [timeout],
  )
}
