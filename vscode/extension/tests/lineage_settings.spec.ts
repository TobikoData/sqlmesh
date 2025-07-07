import { test, expect } from './fixtures'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { openLineageView, SUSHI_SOURCE_PATH } from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Settings button is visible in the lineage view', async ({
  page,
  sharedCodeServer,
}) => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  await page.goto(
    `http://127.0.0.1:${sharedCodeServer.codeServerPort}/?folder=${tempDir}`,
  )

  await page.waitForSelector('text=models')

  // Click on the models folder, excluding external_models
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()
  // Open the waiters.py model
  await page
    .getByRole('treeitem', { name: 'waiters.py', exact: true })
    .locator('a')
    .click()
  await page.waitForSelector('text=Loaded SQLMesh Context')

  // Open lineage
  await openLineageView(page)

  const iframes = page.locator('iframe')
  const iframeCount = await iframes.count()
  let settingsCount = 0

  for (let i = 0; i < iframeCount; i++) {
    const iframe = iframes.nth(i)
    const contentFrame = iframe.contentFrame()
    if (contentFrame) {
      const activeFrame = contentFrame.locator('#active-frame').contentFrame()
      if (activeFrame) {
        try {
          await activeFrame
            .getByRole('button', {
              name: 'Settings',
            })
            .waitFor({ timeout: 1000 })
          settingsCount++
        } catch {
          // Continue to next iframe if this one doesn't have the error
          continue
        }
      }
    }
  }

  expect(settingsCount).toBeGreaterThan(0)
})
