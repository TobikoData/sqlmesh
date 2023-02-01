import { fetchAPI } from './instance'

import type { File } from './client'

export async function saveFileByPath({ path, body = '' }: any) {
  return await fetchAPI<File>({
    url: `/api/files/${path}`,
    method: 'post',
    data: body
  })
}

export async function applyPlan({ body, params }: any) {
  return await fetchAPI({
    url: `/api/plan`,
    method: 'post',
    data: body,
    params
  })
}
