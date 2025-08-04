import { test, expect } from './fixtures'
import fs from 'fs-extra'
import {
  openLineageView,
  openServerPage,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

test('Settings button is visible in the lineage view', async ({
  page,
  sharedCodeServer,
  tempDir,
}) => {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  await openServerPage(page, tempDir, sharedCodeServer)

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
  await waitForLoadedSQLMesh(page)

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
