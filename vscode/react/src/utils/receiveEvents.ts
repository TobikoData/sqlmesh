import type { VSCodeCallback } from '@bus/callbacks'

/**
 * add event listener to the window.
 *
 * returns a function to remove the event listener.
 */
export const addEventListener = <K extends keyof VSCodeCallback>(
  handlerKey: K,
  handler: (payload: VSCodeCallback[K]) => void,
): (() => void) => {
  const handleMessage = (event: MessageEvent) => {
    const { key, payload } = event.data
    if (key === handlerKey) {
      handler(payload)
    }
  }
  window.addEventListener('message', handleMessage)
  return () => {
    window.removeEventListener('message', handleMessage)
  }
}
