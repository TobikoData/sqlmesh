import { sendVSCodeMessage } from '@/utils/vscodeapi'
import { isErr } from '@bus/result'

declare global {
  interface Window {
    __BASE_URL__?: string
  }
}

interface ResponseWithDetail {
  ok: boolean
  detail?: string
}

interface FetchOptionsWithSignal {
  signal?: AbortSignal
}

interface FetchOptions<B extends object = any> {
  url: string
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
  data?: B
  responseType?: string
  headers?: Record<string, string>
  credentials?: 'omit' | 'same-origin' | 'include'
  mode?: 'cors' | 'no-cors' | 'same-origin'
  cache?:
    | 'default'
    | 'no-store'
    | 'reload'
    | 'no-cache'
    | 'force-cache'
    | 'only-if-cached'
  params?: Record<string, string | number | boolean | undefined | null>
}

export async function fetchAPI<T = any, B extends object = any>(
  config: FetchOptions<B>,
  _options?: Partial<FetchOptionsWithSignal>,
): Promise<T & ResponseWithDetail> {
  // Generate a unique ID for this request
  // Create a promise that will resolve when we get a response with matching ID
  return new Promise((resolve, reject) => {
    const requestId = `query_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
    const messageHandler = (event: MessageEvent) => {
      if (
        event.data &&
        event.data.key === 'query_response' &&
        event.data.requestId === requestId
      ) {
        // Remove the listener once we get our response
        window.removeEventListener('message', messageHandler)

        const payload = event.data.payload
        if (isErr(payload)) {
          reject(new Error(payload.error as string))
        } else {
          resolve(payload.value.data)
        }
      }
    }

    // Add the listener
    window.addEventListener('message', messageHandler)

    sendVSCodeMessage('queryRequest', {
      requestId,
      url: config.url,
      params: config.params as any,
      body: config.data,
      method: config.method,
    })

    // Set a timeout to prevent hanging promises
    setTimeout(() => {
      window.removeEventListener('message', messageHandler)
      reject(new Error('Query request timed out'))
    }, 30000) // 30 second timeout
  })
}
