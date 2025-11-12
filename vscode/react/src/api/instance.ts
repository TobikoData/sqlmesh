import { callRpc } from '@/utils/rpc'
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
  const request = {
    url: config.url,
    method: config.method,
    params: config.params,
    body: config.data,
  }
  const result = await callRpc('api_query', request)
  if (isErr(result)) {
    throw new Error(result.error)
  }
  const response = result.value.data
  return response
}
