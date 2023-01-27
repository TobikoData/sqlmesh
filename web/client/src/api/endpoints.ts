import { fetchAPI } from './instance'

export async function saveFileByPath({ path, body = '' }: any) {
  return await fetchAPI<File>({
    url: `/api/files/${path}`,
    method: 'post',
    data: body
  })
}

export async function getContext() {
  return await fetchAPI<{
    models: Array<string>
    engine_adapter: string
    path: string
    dialect: string
    scheduler: string
    time_column_format: string
    concurrent_tasks: number
  }>({ url: '/api/context', method: 'get' })
}
