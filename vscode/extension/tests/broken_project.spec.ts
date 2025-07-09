import { test, expect } from './fixtures'
import fs from 'fs-extra'
import os from 'os'
import path from 'path'
import { openLineageView, saveFile, SUSHI_SOURCE_PATH } from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('bad project, double model', async ({ page, sharedCodeServer }) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Read the customers.sql file
  const customersSql = await fs.readFile(
    path.join(tempDir, 'models', 'customers.sql'),
    'utf8',
  )

  // Write the customers.sql file with a double model
  await fs.writeFile(
    path.join(tempDir, 'models', 'customers_duplicated.sql'),
    customersSql,
  )

  await createPythonInterpreterSettingsSpecifier(tempDir)
  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )

  await page.waitForSelector('text=models')

  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()

  await page.waitForSelector('text=Error creating context')

  await page.waitForTimeout(500)
})

test('working project, then broken through adding double model, then refixed', async ({
  page,
  sharedCodeServer,
}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  await createPythonInterpreterSettingsSpecifier(tempDir)
  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await page.waitForLoadState('networkidle')

  // Open the lineage view to confirm it loads properly
  await openLineageView(page)
  await page.waitForSelector('text=Loaded SQLMesh context')

  // Read the customers.sql file
  const customersSql = await fs.readFile(
    path.join(tempDir, 'models', 'customers.sql'),
    'utf8',
  )

  // Add a duplicate model to break the project
  await fs.writeFile(
    path.join(tempDir, 'models', 'customers_duplicated.sql'),
    customersSql,
  )

  // Open the customers model to trigger the error
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()
  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()
  // Save to refresh the context
  await saveFile(page)

  // Wait for the error to appear
  const iframes = page.locator('iframe')
  const iframeCount = await iframes.count()
  let errorCount = 0

  for (let i = 0; i < iframeCount; i++) {
    const iframe = iframes.nth(i)
    const contentFrame = iframe.contentFrame()
    if (contentFrame) {
      const activeFrame = contentFrame.locator('#active-frame').contentFrame()
      if (activeFrame) {
        try {
          await activeFrame
            .getByText('Error: Failed to load model')
            .waitFor({ timeout: 1000 })
          errorCount++
        } catch {
          // Continue to next iframe if this one doesn't have the error
          continue
        }
      }
    }
  }
  expect(errorCount).toBeGreaterThan(0)

  // Remove the duplicated model to fix the project
  await fs.remove(path.join(tempDir, 'models', 'customers_duplicated.sql'))

  // Save again to refresh the context
  await saveFile(page)

  const iframes2 = page.locator('iframe')
  const iframeCount2 = await iframes2.count()
  let raw_demographicsCount = 0

  for (let i = 0; i < iframeCount2; i++) {
    const iframe = iframes2.nth(i)
    const contentFrame = iframe.contentFrame()
    if (contentFrame) {
      const activeFrame = contentFrame.locator('#active-frame').contentFrame()
      if (activeFrame) {
        try {
          await activeFrame
            .getByText('sushi.customers')
            .waitFor({ timeout: 1000 })
          raw_demographicsCount++
        } catch {
          // Continue to next iframe if this one doesn't have the error
          continue
        }
      }
    }
  }
  expect(raw_demographicsCount).toBeGreaterThan(0)
})

test('bad project, double model, then fixed', async ({
  page,
  sharedCodeServer,
}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Read the customers.sql file
  const customersSql = await fs.readFile(
    path.join(tempDir, 'models', 'customers.sql'),
    'utf8',
  )

  // Write the customers.sql file with a double model
  await fs.writeFile(
    path.join(tempDir, 'models', 'customers_duplicated.sql'),
    customersSql,
  )

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await page.waitForLoadState('networkidle')
  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )

  await page.waitForSelector('text=models')

  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()

  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()

  await page.waitForSelector('text=Error creating context')

  // Remove the duplicated model
  await fs.remove(path.join(tempDir, 'models', 'customers_duplicated.sql'))

  // Open the linage view
  await openLineageView(page)

  // Wait for the error to go away
  const iframes = page.locator('iframe')
  const iframeCount = await iframes.count()
  let raw_demographicsCount = 0

  for (let i = 0; i < iframeCount; i++) {
    const iframe = iframes.nth(i)
    const contentFrame = iframe.contentFrame()
    if (contentFrame) {
      const activeFrame = contentFrame.locator('#active-frame').contentFrame()
      if (activeFrame) {
        try {
          await activeFrame
            .getByText('sushi.customers')
            .waitFor({ timeout: 1000 })
          raw_demographicsCount++
        } catch {
          continue
        }
      }
    }
  }
  expect(raw_demographicsCount).toBeGreaterThan(0)
})

test('bad project, double model, check lineage', async ({
  page,
  sharedCodeServer,
}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  // Read the customers.sql file
  const customersSql = await fs.readFile(
    path.join(tempDir, 'models', 'customers.sql'),
    'utf8',
  )

  // Write the customers.sql file with a double model
  await fs.writeFile(
    path.join(tempDir, 'models', 'customers_duplicated.sql'),
    customersSql,
  )

  await createPythonInterpreterSettingsSpecifier(tempDir)
  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )
  await page.waitForLoadState('networkidle')

  // Open the lineage view
  await openLineageView(page)

  await page.waitForSelector('text=Error creating context')
  await page.waitForSelector('text=Error:')

  await page.waitForTimeout(500)
})
