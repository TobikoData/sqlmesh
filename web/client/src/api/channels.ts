import { isNil } from '../utils'

type ChannelCallback = (
  data: any,
  channel: EventSource,
  unsubscribe: () => void,
) => void

const channels = new Map<string, EventSource>()

export function useChannel(
  topic: string,
  callback: ChannelCallback,
): [() => void, () => EventSource | undefined, () => boolean] {
  return [
    () => subscribe(topic, callback),
    () => channels.get(topic),
    () => channels.delete(topic),
  ]
}

function subscribe(
  topic: string,
  callback: ChannelCallback,
): EventSource | undefined {
  if (isNil(topic)) return

  cleanUpChannel(topic, callback)

  channels.set(topic, getEventSource(topic))

  const channel = channels.get(topic)

  channel?.addEventListener('message', handleChannelMessage(topic, callback))

  return channel
}

function cleanUpChannel(topic: string, callback: ChannelCallback): void {
  const channel = channels.get(topic)

  channel?.close()
  channel?.removeEventListener('message', handleChannelMessage(topic, callback))

  channels.delete(topic)
}

function handleChannelMessage(
  topic: string,
  callback: ChannelCallback,
): (e: MessageEvent) => void {
  return (event: MessageEvent) => {
    if (isNil(topic) || isNil(callback) || isNil(event.data)) return

    const channel = channels.get(topic)

    if (channel == null) return

    callback(JSON.parse(event.data), channel, () => channels.delete(topic))
  }
}

function getEventSource(topic: string): EventSource {
  return new EventSource(topic)
}
