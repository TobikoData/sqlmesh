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
  if (topic == null) return

  let channel = channels.get(topic)

  if (channel != null) {
    channel.close()
  }

  channels.set(topic, getEventSource(topic))

  channel = channels.get(topic)

  if (callback == null || channel == null) return

  channel.onmessage = handleChannelMessage(topic, callback)
}

function handleChannelMessage(topic: string, callback: ChannelCallback): (e: MessageEvent) => void {
  return (event: MessageEvent) => {
    if (topic == null || callback == null || event.data == null) return

    const channel = channels.get(topic)

    if (channel == null) return

    callback(JSON.parse(event.data), channel, () => channels.delete(topic))
  }
}

function getEventSource(topic: string): EventSource {
  return new EventSource(topic)
}
