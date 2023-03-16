import { tableFromIPC } from 'apache-arrow'

const baseURL = window.location.origin

export interface ResponseWithDetail {
  ok: boolean
  detail?: string
}

export interface FetchOptions<B extends object = any> {
  url: string
  method: 'get' | 'post' | 'put' | 'delete' | 'patch'
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
  params?: Record<string, string>
  signal?: AbortSignal
}

export async function fetchAPI<T = any, B extends object = any>(
  options: FetchOptions<B>,
): Promise<T & ResponseWithDetail> {
  const {
    url,
    method,
    params,
    data,
    headers,
    credentials,
    mode,
    cache,
    signal,
  } = options
  const hasSearchParams = Object.keys({ ...params }).length > 0
  const fullUrl = url.replace(/([^:]\/)\/+/g, '$1')
  const input = new URL(fullUrl, baseURL)

  if (hasSearchParams) {
    const searchParams: Record<string, string> = Object.entries({
      ...params,
    }).reduce((acc: Record<string, string>, [key, param]) => {
      if (param != null) {
        acc[key] = param
      }

      return acc
    }, {})
    input.search = new URLSearchParams(searchParams).toString()
  }

  return await new Promise<T & ResponseWithDetail>((resolve, reject) => {
    fetch(input, {
      method,
      credentials,
      mode,
      cache,
      headers: toRequestHeaders(headers),
      body: toRequestBody(data),
      signal,
    })
      .then(async response => {
        const headerContentType = response.headers.get('Content-Type')

        if (headerContentType == null)
          return { ok: false, detail: 'Empty response' }
        if (response.status === 204) return { ok: true }
        if (response.status >= 400)
          return { ok: false, ...(await response.json()) }

        const isEventStream = headerContentType.includes('text/event-stream')
        const isApplicationJson = headerContentType.includes('application/json')
        const isArrowStream = headerContentType.includes(
          'application/vnd.apache.arrow.stream',
        )

        if (isApplicationJson) return await response.json()
        if (isArrowStream) return await tableFromIPC(response)
        if (isEventStream) {
          const text = await response.text()
          const message = text.trim().replace('data: ', '').trim()

          return JSON.parse(message)
        }
      })
      .then(resolve)
      .catch(reject)
  })
}

function toRequestHeaders(headers?: Record<string, string>): HeadersInit {
  return {
    'Content-Type': 'application/json',
    ...(headers ?? {}),
  }
}

function toRequestBody(obj: unknown): BodyInit {
  try {
    return JSON.stringify(obj)
  } catch (error) {
    return ''
  }
}

export default fetchAPI
