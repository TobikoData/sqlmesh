import { isNil } from '../utils'

const SSE_CHANNEL = getEventSource('/api/events')

type ChannelCallback = (
  data: any,
  // channel: EventSource,
  // unsubscribe: () => void,
) => void

// const channels = new Map<string, EventSource>()

export function useChannelEvents(callback: ChannelCallback): [
  (topic: string) => void,
  // () => EventSource | undefined,
  // () => boolean
] {
  return [
    (topic: string) => subscribe(topic, callback),
    // () => channels.get(topic),
    // () => channels.delete(topic),
  ]
}

function subscribe(
  topic: string,
  callback: ChannelCallback,
): Optional<() => void> {
  if (isNil(topic)) return

  // cleanUpChannel(topic, callback)

  // channels.set(topic, callback)

  // const channel = channels.get(topic)

  // channel?.addEventListener('message', handleChannelMessage(topic, callback))
  SSE_CHANNEL.addEventListener(topic, handleChannelTasks(topic, callback))

  return () => {
    SSE_CHANNEL.removeEventListener(topic, handleChannelTasks(topic, callback))
  }
}

// function cleanUpChannel(topic: string, callback: ChannelCallback): void {
//   const channel = channels.get(topic)

//   channel?.close()
//   // channel?.removeEventListener('message', handleChannelMessage(topic, callback))
//   channel?.removeEventListener(topic, handleChannelTasks(topic, callback))

//   channels.delete(topic)
// }

function handleChannelTasks(
  topic: string,
  callback: ChannelCallback,
): (e: MessageEvent) => void {
  return (event: MessageEvent) => {
    if (isNil(topic) || isNil(callback) || isNil(event.data)) return

    // const channel = channels.get(topic)

    // if (channel == null) return

    try {
      console.log(JSON.parse(event.data))
      callback(JSON.parse(event.data))
    } catch (error) {
      console.warn(error)
    }
  }
}

// function handleChannelMessage(
//   topic: string,
//   callback: ChannelCallback,
// ): (e: MessageEvent) => void {
//   return (event: MessageEvent) => {
//     if (isNil(topic) || isNil(callback) || isNil(event.data)) return

//     const channel = channels.get(topic)

//     if (channel == null) return

//     const data = {
//       ok: true,
//       updated_at: new Date().toISOString(),
//       message: event.data,
//     }

//     callback(data, channel, () => channels.delete(topic))
//   }
// }

function getEventSource(topic: string): EventSource {
  return new EventSource(topic)
}
