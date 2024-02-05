import { expect, type Page } from '@playwright/test'

export async function testHeader(page: Page): Promise<void> {
  console.log('Testing header')
  await expect(page.getByTitle('Home', { exact: true })).toBeVisible()
  await expect(page.getByText('Documentation', { exact: true })).toBeVisible()
  await expect(
    page.getByRole('link', { name: 'Tobiko Data logo' }),
  ).toBeVisible()
}

export async function testFooter(page: Page): Promise<void> {
  console.log('Testing footer')
  await expect(
    page.getByRole('link', { name: 'Tobiko Data, Inc.' }),
  ).toBeVisible()
  await expect(
    page.getByTitle('SQLMesh Version', { exact: true }),
  ).toBeVisible()
}

export async function testModulesAPI(
  page: Page,
  opt: { modules: string[] },
  promise: Promise<any> = page.waitForResponse('/api/modules'),
): Promise<void> {
  console.log('Testing modules API')
  const api = await promise
  expect(api.status()).toBe(200)
  const apiBody = await api.json()
  expect(apiBody.length).toBeGreaterThan(0)
  expect(apiBody.length).toEqual(opt.modules.length)
}

export async function testMetaAPI(
  page: Page,
  promise?: Promise<any>,
): Promise<void> {
  console.log('Testing meta API')
  promise = promise ?? page.waitForResponse('/api/meta')
  const api = await promise
  expect(api.status()).toBe(200)
  const apiBody = await api.json()
  expect(apiBody.has_running_task).toEqual(false)
}

export async function testEventsAPI(
  page: Page,
  promise?: Promise<any>,
): Promise<void> {
  console.log('Testing events API')
  promise = promise ?? page.waitForResponse('/api/events')
  const api = await promise
  expect(api.status()).toBe(200)
}

export async function testFilesAPI(
  page: Page,
  opt: { projectName: string },
  promise?: Promise<any>,
): Promise<void> {
  console.log('Testing files API')
  promise = promise ?? page.waitForResponse('/api/files')
  const api = await promise
  expect(api.status()).toBe(200)
  const apiBody = await api.json()
  expect(apiBody.path).toEqual('')
  expect(apiBody.name).toEqual(opt.projectName)
}

export async function testModelsAPI(
  page: Page,
  promise?: Promise<any>,
): Promise<void> {
  console.log('Testing models API')
  promise = promise ?? page.waitForResponse('/api/models')
  const api = await promise
  expect(api.status()).toBe(200)
}

export async function testEnvironmentsAPI(
  page: Page,
  promise?: Promise<any>,
): Promise<void> {
  console.log('Testing environments API')
  promise = promise ?? page.waitForResponse('/api/environments')
  const api = await promise
  expect(api.status()).toBe(200)
  const apiBody = await api.json()
  expect(apiBody.environments.prod.name).toEqual('prod')
  expect(apiBody.environments.prod.plan_id).toEqual('')
}

export async function testPlanAPI(
  page: Page,
  promise?: Promise<any>,
): Promise<void> {
  console.log('Testing plan API')
  promise = promise ?? page.waitForResponse('/api/plan')
  const api = await promise
  expect(api.status()).toBe(204)
}

export async function testHistoryNavigation(
  page: Page,
  {
    hasBack = true,
    hasForward = true,
  }: {
    hasBack: boolean
    hasForward: boolean
  },
): Promise<void> {
  console.log('Checking history navigation')
  const el = page.getByTestId('history-navigation')
  await expect(el).toBeVisible()
  await expect(el.getByTitle('Go Back', { exact: true })).toBeVisible()
  if (hasBack) {
    await expect(el.getByTitle('Go Back', { exact: true })).toBeEnabled()
  } else {
    await expect(el.getByTitle('Go Back', { exact: true })).toBeDisabled()
  }
  await expect(el.getByTitle('Go Forward', { exact: true })).toBeVisible()
  if (hasForward) {
    await expect(el.getByTitle('Go Forward', { exact: true })).toBeEnabled()
  } else {
    await expect(el.getByTitle('Go Forward', { exact: true })).toBeDisabled()
  }
}

export async function testEnvironmentDetails(
  page: Page,
  {
    env = 'prod',
    action = 'Plan',
    isDisabledEnv = false,
    isDisabledAction = false,
  }: {
    env: string
    action?: string
    isDisabledEnv?: boolean
    isDisabledAction?: boolean
  },
): Promise<void> {
  console.log('Checking environment details')
  const el = page.getByTestId('environment-details')
  await expect(el).toBeVisible()
  await expect(el.getByRole('link', { name: action })).toBeVisible({
    timeout: 20000,
  })
  if (isDisabledAction) {
    await expect(el.getByRole('link', { name: action })).toBeDisabled()
  } else {
    await expect(el.getByRole('link', { name: action })).toBeEnabled()
  }
  await expect(el.getByRole('button', { name: env })).toBeVisible({
    timeout: 20000,
  })
  if (isDisabledEnv) {
    await expect(el.getByRole('button', { name: env })).toBeDisabled()
  } else {
    await expect(el.getByRole('button', { name: env })).toBeEnabled()
  }
}

export async function testErrors(
  page: Page,
  { label = 'No Errors' }: { label: string },
): Promise<void> {
  console.log('Checking errors')
  const el = page.getByTestId('report-errors')
  await expect(el).toBeVisible()
  await expect(el).toHaveText(label)
}

export async function testModuleNavigation(page: Page): Promise<void> {
  console.log('Checking module navigation')
  const el = page.getByTestId('module-navigation')
  await expect(el).toBeVisible()
  await expect(el.getByTitle('File Explorer')).toBeVisible()
  await expect(el.getByTitle('Docs')).toBeVisible()
  await expect(el.getByTitle('Errors')).toBeVisible()
  await expect(el.getByTitle('Plan')).toBeVisible()
}

export async function testChangesAndBackfills(
  page: Page,
  {
    backfills = 0,
    added = 0,
    removed = 0,
    metadata = 0,
    direct = 0,
    indirect = 0,
  }: {
    backfills: number
    added?: number
    removed?: number
    metadata?: number
    direct?: number
    indirect?: number
  },
): Promise<void> {
  console.log('Checking environment changes')
  const el = page.getByTestId('environment-changes')
  await expect(el).toBeVisible({ timeout: 20000 })
  await expect(el.getByText('Changes')).toBeVisible()
  if (added > 0) {
    await expect(el.getByTitle('Added', { exact: true })).toHaveText(
      String(added),
    )
  }
  if (removed > 0) {
    await expect(el.getByTitle('Removed', { exact: true })).toHaveText(
      String(removed),
    )
  }
  if (metadata > 0) {
    await expect(el.getByTitle('Metadata', { exact: true })).toHaveText(
      String(metadata),
    )
  }
  if (direct > 0) {
    await expect(
      el.getByTitle('Directly Modified', { exact: true }),
    ).toHaveText(String(direct))
  }
  if (indirect > 0) {
    await expect(
      el.getByTitle('Indirectly Modified', { exact: true }),
    ).toHaveText(String(indirect))
  }
  if (backfills > 0) {
    await expect(el.getByTitle('Backfills', { exact: true })).toHaveText(
      String(backfills),
    )
  }
}

export async function testEditor(page: Page): Promise<void> {
  console.log('Checking editor')
  const elEditor = page.getByTestId('editor')
  await expect(elEditor).toBeVisible()
}

export async function testEditorTabs(page: Page): Promise<void> {
  console.log('Checking editor tabs')
  const elEditorTabs = page.getByTestId('editor-tabs')
  await expect(elEditorTabs).toBeVisible()
}

export async function testFileExplorer(page: Page): Promise<void> {
  console.log('Checking file explorer')
  const elFileExplorer = page.getByTestId('file-explorer')
  await expect(elFileExplorer).toBeVisible()
}
