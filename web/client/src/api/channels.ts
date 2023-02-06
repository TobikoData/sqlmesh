import { isNil } from "../utils"

const channels = new Map<string, EventSource>()

export function useChannel(topic: string, callback: any): [() => void, () => EventSource | undefined, () => boolean] {
  return [
    () => subscribe(topic, callback),
    () => channels.get(topic),
    () => channels.delete(topic)
  ]
}

function subscribe(topic: string, callback: any) {
  if (isNil(topic)) return

  let channel = channels.get(topic)

  channel?.close()

  channels.set(topic, getEventSource(topic))

  channel = channels.get(topic)

  if (channel == null) return

  channel.onmessage = (event: any) => {
    callback && callback(JSON.parse(event.data), channel, () => channels.delete(topic))
  }
}

function getEventSource(topic: string) {
  return new EventSource(topic)
}
