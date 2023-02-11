import { isNil } from '../utils'

type ChannelCallback = (data: any, channel: EventSource, unsubscribe: () => void) => void

const channels = new Map<string, EventSource>()

export function useChannel(
  topic: string,
  callback: ChannelCallback
): [() => void, () => EventSource | undefined, () => boolean] {
  return [
    () => {
      subscribe(topic, callback)
    },
    () => channels.get(topic),
    () => channels.delete(topic),
  ]
}

function subscribe(topic: string, callback: ChannelCallback): void {
  if (isNil(topic)) return

  let channel = channels.get(topic)

  if (channel == null) return

  channel.close()

  channels.set(topic, getEventSource(topic))

  channel = channels.get(topic)

  if (channel == null) return

  channel.onmessage = (event: MessageEvent) => {
    if (callback == null || channel == null) return

    callback(JSON.parse(event.data), channel, () => channels.delete(topic))
  }
}

function getEventSource(topic: string): EventSource {
  return new EventSource(topic)
}
