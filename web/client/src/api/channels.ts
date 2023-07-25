import { isFalse, isNil, isNotNil } from '../utils'

type ChannelCallback<TData = any> = (data: TData) => void

const DELAY_AND_RECONNECT = 20000
const DELAY_AND_RETRY = 3000

class EventSourceConnection {
  channels = new Map<string, Optional<(e: MessageEvent) => void>>()
  eventSource: Optional<EventSource>
  timerId: Optional<ReturnType<typeof setTimeout>>
  source: string
  delay: number = DELAY_AND_RECONNECT

  constructor(source: string, delay?: number) {
    this.source = source
    this.delay = delay ?? this.delay

    this.setEventSource()
  }

  listen(eventSource: EventSource): void {
    eventSource.addEventListener('error', (e: MessageEvent) => {
      this.reconnect(DELAY_AND_RETRY)
    })

    eventSource.addEventListener('ping', (e: MessageEvent) => {
      this.reconnect(this.delay)
    })
  }

  cleanup(): void {
    this.channels.forEach((handler, topic) => {
      if (isNotNil(handler)) {
        this.eventSource?.removeEventListener(topic, handler)
      }
    })

    this.eventSource?.close()

    this.eventSource = undefined
  }

  resubscribe(): void {
    this.channels.forEach((handler, topic) => {
      if (isNotNil(handler)) {
        this.eventSource?.addEventListener(topic, handler)
      }
    })
  }

  reconnect(delay: number): void {
    clearTimeout(this.timerId)

    this.timerId = setTimeout(() => {
      console.log(`Reconnecting Event Source ${this.source}`)
      this.cleanup()
      this.resubscribe()
      this.setEventSource()
    }, delay)
  }

  getEventSource(): EventSource {
    return new EventSource(this.source, {
      withCredentials: true,
    })
  }

  setEventSource(): void {
    this.eventSource = this.getEventSource()
    this.listen(this.eventSource)
  }

  addChannel<TData = any>(
    topic: string,
    callback: ChannelCallback<TData>,
  ): void {
    const handler = this.getChannelHandler(topic, callback)

    this.eventSource?.addEventListener(topic, handler)
    this.channels.set(topic, handler)
  }

  removeChannel(topic: string): void {
    const handler = this.getChannel(topic)

    if (isNotNil(handler)) {
      this.eventSource?.removeEventListener(topic, handler)
      this.channels.delete(topic)
    }
  }

  hasChannel(topic: string): boolean {
    return isNotNil(this.getChannel(topic))
  }

  getChannel(topic: string): Optional<(e: MessageEvent) => void> {
    return this.channels.get(topic)
  }

  private getChannelHandler<TData = any>(
    topic: string,
    callback: ChannelCallback<TData>,
  ): (e: MessageEvent) => void {
    return (event: MessageEvent<string>) => {
      this.reconnect(this.delay)

      if (isNil(topic) || isNil(callback) || isNil(event.data)) return

      try {
        callback(JSON.parse(event.data))
      } catch (error) {
        console.warn(error)
      }
    }
  }
}

const Connection = new EventSourceConnection('/api/events', DELAY_AND_RECONNECT)

export function useChannelEvents(): <TData = any>(
  topic: string,
  callback: ChannelCallback<TData>,
) => Optional<() => void> {
  return (topic, callback) => {
    if (isNotNil(topic) && isFalse(Connection.hasChannel(topic))) {
      Connection.addChannel(topic, callback)
    }

    return () => {
      Connection.removeChannel(topic)
    }
  }
}
