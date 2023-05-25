import { isNil } from '../utils'

type ChannelCallback<TData = any> = (data: TData) => void

const SSE_CHANNEL = getEventSource('/api/events')

const CHANNELS = new Map<string, Optional<() => void>>()

export function useChannelEvents(): <TData = any>(
  topic: string,
  callback: ChannelCallback<TData>,
) => Optional<() => void> {
  return (topic, callback) => subscribe(topic, callback)
}

function subscribe<TData = any>(
  topic: string,
  callback: ChannelCallback<TData>,
): Optional<() => void> {
  if (isNil(topic) || CHANNELS.has(topic)) return CHANNELS.get(topic)

  const handler = handleChannelTopicCallback<TData>(topic, callback)

  SSE_CHANNEL.addEventListener(topic, handler)

  CHANNELS.set(topic, () => {
    SSE_CHANNEL.removeEventListener(topic, handler)
    CHANNELS.delete(topic)
  })

  return CHANNELS.get(topic)
}

function handleChannelTopicCallback<TData = any>(
  topic: string,
  callback: ChannelCallback<TData>,
): (e: MessageEvent) => void {
  return (event: MessageEvent<string>) => {
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
