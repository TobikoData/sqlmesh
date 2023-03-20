import { isNil } from '../utils'

type ChannelCallback = (data: any) => void

const SSE_CHANNEL = getEventSource('/api/events')

const CHANNELS = new Map<string, Optional<() => void>>()

export function useChannelEvents(): [
  (topic: string, callback: ChannelCallback) => Optional<() => void>,
] {
  return [
    (topic: string, callback: ChannelCallback) => subscribe(topic, callback),
  ]
}

function subscribe(
  topic: string,
  callback: ChannelCallback,
): Optional<() => void> {
  if (isNil(topic) || CHANNELS.has(topic)) return CHANNELS.get(topic)

  const handler = handleChannelTasks(topic, callback)

  SSE_CHANNEL.addEventListener(topic, handler)

  CHANNELS.set(topic, () => {
    SSE_CHANNEL.removeEventListener(topic, handler)
    CHANNELS.delete(topic)
  })

  return CHANNELS.get(topic)
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

function getEventSource(source: string): EventSource {
  return new EventSource(source)
}
