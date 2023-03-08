import { isNil } from '../utils'

type ChannelCallback = (data: any) => void

const SSE_CHANNEL = getEventSource('/api/events')

export function useChannelEvents(
  callback: ChannelCallback,
): [(topic: string) => void] {
  return [(topic: string) => subscribe(topic, callback)]
}

function subscribe(
  topic: string,
  callback: ChannelCallback,
): Optional<() => void> {
  if (isNil(topic)) return

  SSE_CHANNEL.addEventListener(topic, handleChannelTasks(topic, callback))

  return () => {
    SSE_CHANNEL.removeEventListener(topic, handleChannelTasks(topic, callback))
  }
}

function handleChannelTasks(
  topic: string,
  callback: ChannelCallback,
): (e: MessageEvent) => void {
  return (event: MessageEvent) => {
    if (isNil(topic) || isNil(callback) || isNil(event.data)) return

    try {
      callback(JSON.parse(event.data))
    } catch (error) {
      console.warn(error)
    }
  }
}

function getEventSource(topic: string): EventSource {
  return new EventSource(topic)
}
