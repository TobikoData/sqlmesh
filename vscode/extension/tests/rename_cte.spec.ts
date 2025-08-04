import { test, expect, Page } from './fixtures'
import fs from 'fs-extra'
import {
  findAllReferences,
  openServerPage,
  renameSymbol,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

async function setupTestEnvironment({
  page,
  sharedCodeServer,
  tempDir,
}: {
  page: Page
  sharedCodeServer: any
  tempDir: string
}) {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)

  await openServerPage(page, tempDir, sharedCodeServer)

  // Navigate to customers.sql which contains CTEs
  await page.waitForSelector('text=models')
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()
  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()
  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)
}

test.describe('CTE Rename', () => {
  test('Rename CTE from definition', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupTestEnvironment({ page, sharedCodeServer, tempDir })
    // Click on the inner CTE definition "current_marketing" (not the outer one)
    await page.locator('text=WITH current_marketing AS').click({
      position: { x: 100, y: 5 },
    })

    // Open rename
    await renameSymbol(page)
    await page.waitForSelector('text=Rename')
    await page.waitForSelector('input:focus')

    // Type new name and confirm
    await page.keyboard.type('new_marketing')
    await page.keyboard.press('Enter')

    // Verify the rename was applied
    await page.waitForSelector('text=WITH new_marketing AS')
  })

  test('Rename CTE from usage', async ({ page, sharedCodeServer, tempDir }) => {
    await setupTestEnvironment({ page, sharedCodeServer, tempDir })
    // Click on CTE usage in FROM clause
    await page.locator('text=FROM current_marketing_outer').click({
      position: { x: 80, y: 5 },
    })

    // Open rename
    await renameSymbol(page)
    await page.waitForSelector('text=Rename')
    await page.waitForSelector('input:focus')

    // Type new name
    await page.keyboard.type('updated_marketing_out')

    // Confirm rename
    await page.keyboard.press('Enter')

    await page.waitForSelector('text=WITH updated_marketing_out AS')
    await page.waitForSelector('text=FROM updated_marketing_out')
  })

  test('Cancel CTE rename', async ({ page, sharedCodeServer, tempDir }) => {
    await setupTestEnvironment({ page, sharedCodeServer, tempDir })
    // Click on the CTE to rename
    await page.locator('text=current_marketing_outer').first().click()

    // Open rename
    await renameSymbol(page)
    await page.waitForSelector('text=Rename')
    await page.waitForSelector('input:focus')

    // Type new name but cancel
    await page.keyboard.type('cancelled_name')
    await page.keyboard.press('Escape')

    // Wait for UI to update
    await page.waitForTimeout(500)

    // Verify CTE name was NOT changed
    await expect(
      page.locator('text=current_marketing_outer').first(),
    ).toBeVisible()
    await expect(page.locator('text=cancelled_name')).not.toBeVisible()
  })

  test('Rename CTE updates all references', async ({
    page,
    tempDir,
    sharedCodeServer,
  }) => {
    await setupTestEnvironment({ page, sharedCodeServer, tempDir })
    // Click on the CTE definition
    await page.locator('text=WITH current_marketing AS').click({
      position: { x: 100, y: 5 },
    })

    // Open rename
    await renameSymbol(page)
    await page.waitForSelector('text=Rename')
    await page.waitForSelector('input:focus')

    // Type new name and confirm
    await page.keyboard.type('renamed_cte')
    await page.keyboard.press('Enter')

    // Click on the renamed CTE
    await page.locator('text=WITH renamed_cte AS').click({
      position: { x: 100, y: 5 },
    })

    // Find all references using keyboard shortcut
    await findAllReferences(page)

    // Verify references panel shows all occurrences
    await page.waitForSelector('text=References')
    await expect(page.locator('text=customers.sql').first()).toBeVisible()
    await page.waitForSelector('text=WITH renamed_cte AS')
    await page.waitForSelector('text=renamed_cte.*')
    await page.waitForSelector('text=FROM renamed_cte')
    await page.waitForSelector('text=renamed_cte.customer_id != 100')
  })

  test('Rename CTE with preview', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupTestEnvironment({ page, sharedCodeServer, tempDir })
    // Click on the CTE to rename
    await page.locator('text=WITH current_marketing AS').click({
      position: { x: 100, y: 5 },
    })

    // Open rename
    await renameSymbol(page)
    await page.waitForSelector('text=Rename')
    await page.waitForSelector('input:focus')

    // Type new name
    await page.keyboard.type('preview_marketing')

    // Press Cmd+Enter (Meta+Enter) to preview changes
    await page.keyboard.press(
      process.platform === 'darwin' ? 'Meta+Enter' : 'Control+Enter',
    )

    // Verify preview UI is showing
    await expect(page.locator('text=Refactor Preview').first()).toBeVisible()
    await expect(page.locator('text=Apply').first()).toBeVisible()
    await expect(page.locator('text=Discard').first()).toBeVisible()

    // Verify the preview shows both old and new names
    await expect(page.locator('text=current_marketing').first()).toBeVisible()
    await expect(page.locator('text=preview_marketing').first()).toBeVisible()

    // Apply the changes
    await page.locator('text=Apply').click()

    // Verify the rename was applied
    await expect(page.locator('text=WITH preview_marketing AS')).toBeVisible()
  })
})
