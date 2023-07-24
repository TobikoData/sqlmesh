import { isNil, isNotNil } from '../utils'

type ChannelCallback<TData = any> = (data: TData) => void

class EventSourceConnection {
  channels = new Map<string, Optional<(e: MessageEvent) => void>>()
  eventSource: Optional<EventSource>
  timerId: Optional<ReturnType<typeof setTimeout>>
  source: string
  wait: number = 3000

  constructor(source: string, wait?: number) {
    this.source = source
    this.wait = wait ?? this.wait

    this.setEventSource()
  }

  listen(eventSource: EventSource): void {
    eventSource.addEventListener('error', (e: MessageEvent) => {
      this.reconnect('error', 3000)
    })

    eventSource.addEventListener('ping', (e: MessageEvent) => {
      this.reconnect('ping', this.wait)
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

  reconnect(topic: string, wait: number): void {
    console.log('From: ', topic)

    clearTimeout(this.timerId)

    this.timerId = setTimeout(() => {
      console.log(`Reconnecting Event Source ${this.source}`)
      this.cleanup()
      this.resubscribe()
      this.setEventSource()
    }, wait)
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
    const handler = this.channels.get(topic)

    if (isNotNil(handler)) {
      this.eventSource?.removeEventListener(topic, handler)
      this.channels.delete(topic)
    }
  }

  hasChannel(topic: string): boolean {
    return isNotNil(this.channels.get(topic))
  }

  getChannel(topic: string): Optional<(e: MessageEvent) => void> {
    return this.channels.get(topic)
  }

  private getChannelHandler<TData = any>(
    topic: string,
    callback: ChannelCallback<TData>,
  ): (e: MessageEvent) => void {
    return (event: MessageEvent<string>) => {
      this.reconnect(topic, this.wait)

      if (isNil(topic) || isNil(callback) || isNil(event.data)) return

      try {
        callback(JSON.parse(event.data))
      } catch (error) {
        console.warn(error)
      }
    }
  }
}

const Connection = new EventSourceConnection('/api/events', 15000)

export function useChannelEvents(): <TData = any>(
  topic: string,
  callback: ChannelCallback<TData>,
) => Optional<() => void> {
  return (topic, callback) => {
    if (isNil(topic) || Connection.hasChannel(topic)) return cleanUpChannel

    Connection.addChannel(topic, callback)

    return cleanUpChannel

    function cleanUpChannel(): void {
      Connection.removeChannel(topic)
    }
  }
}
