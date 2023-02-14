import { fetchAPI } from './instance'

import type { File } from './client'

export async function saveFileByPath<T extends object>({
  path,
  body,
}: {
  path: string
  body: T
}): Promise<File> {
  return await fetchAPI<File & { ok: boolean }, T>({
    url: `/api/files/${path}`,
    method: 'post',
    data: body,
  })
}

export async function applyPlan<T extends object>({
  body,
  params,
}: {
  body: T
  params: Record<string, string>
}): Promise<void> {
  await fetchAPI({
    url: `/api/plan`,
    method: 'post',
    data: body,
    params,
  })
}
