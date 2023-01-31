import { fetchAPI } from './instance'

import type { File } from './client'

export async function saveFileByPath({ path, body = '' }: any) {
  return await fetchAPI<File>({
    url: `/api/files/${path}`,
    method: 'post',
    data: body
  })
}
