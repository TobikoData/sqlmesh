import { isNil, isStringEmptyOrNil } from '@utils/index'
import { tableFromIPC } from 'apache-arrow'

declare global {
  interface Window {
    __BASE_URL__?: string
  }
}

export interface ResponseWithDetail {
  ok: boolean
  detail?: string
}

export interface FetchOptionsWithSignal {
  signal?: AbortSignal
}

export interface FetchOptions<B extends object = any> {
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
  params?: Record<string, Maybe<string | number | boolean>>
}

export async function fetchAPI<T = any, B extends object = any>(
  config: FetchOptions<B>,
  options?: Partial<FetchOptionsWithSignal>,
): Promise<T & ResponseWithDetail> {
  const { url, method, params, data, headers, credentials, mode, cache } =
    config
  const hasSearchParams = Object.keys({ ...params }).length > 0
  const fullUrl = url.replace(/([^:]\/)\/+/g, '$1')
  const input = new URL(getUrlWithPrefix(fullUrl), window.location.origin)

  if (hasSearchParams) {
    const searchParams: Record<string, string> = Object.entries({
      ...params,
    }).reduce((acc: Record<string, string>, [key, param]) => {
      if (param != null) {
        acc[key] = String(param)
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
      signal: options?.signal,
    })
      .then(async response => {
        if (response.status === 204) return { ok: true }

        const headerContentType = response.headers.get('Content-Type')
        if (isNil(headerContentType))
          return { ok: false, message: 'Empty response' }
        if (response.status >= 400) {
          try {
            const json = await response.json()

            json.status = json.status ?? response.status

            throw json
          } catch (error: any) {
            throw {
              message: response.statusText,
              ...error,
            } as unknown as Error
          }
        }

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

export function getUrlWithPrefix(url: string = '/'): string {
  let urlWithPrefix = `${
    isStringEmptyOrNil(window.__BASE_URL__) ? '/' : window.__BASE_URL__
  }/${url}`

  while (urlWithPrefix.includes('//')) {
    urlWithPrefix = urlWithPrefix.replaceAll('//', '/')
  }

  return urlWithPrefix
}

export default fetchAPI
