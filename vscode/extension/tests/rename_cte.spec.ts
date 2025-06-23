import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

// Keyboard shortcuts
const RENAME_KEY = 'F2'
const FIND_ALL_REFERENCES_KEY =
  process.platform === 'darwin' ? 'Alt+Shift+F12' : 'Ctrl+Shift+F12'

// Helper function to set up a test environment
async function setupTestEnvironment() {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  const { window, close } = await startVSCode(tempDir)

  // Navigate to customers.sql which contains CTEs
  await window.waitForSelector('text=models')
  await window
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()
  await window
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()
  await window.waitForSelector('text=grain')
  await window.waitForSelector('text=Loaded SQLMesh Context')

  return { window, close, tempDir }
}

test.describe('CTE Rename', () => {
  test('Rename CTE from definition', async () => {
    const { window, close, tempDir } = await setupTestEnvironment()

    try {
      // Click on the inner CTE definition "current_marketing" (not the outer one)
      await window.locator('text=WITH current_marketing AS').click({
        position: { x: 100, y: 5 },
      })

      // Press F2 to trigger rename
      await window.keyboard.press(RENAME_KEY)
      await expect(window.locator('text=Rename')).toBeVisible()
      const renameInput = window.locator('input:focus')
      await expect(renameInput).toBeVisible()

      // Type new name and confirm
      await window.keyboard.type('new_marketing')
      await window.keyboard.press('Enter')
      await window.waitForTimeout(1000)

      // Verify the rename was applied
      await expect(window.locator('text=WITH new_marketing AS')).toBeVisible()
    } finally {
      await close()
      await fs.remove(tempDir)
    }
  })

  test('Rename CTE from usage', async () => {
    const { window, close, tempDir } = await setupTestEnvironment()

    try {
      // Click on CTE usage in FROM clause
      await window.locator('text=FROM current_marketing_outer').click({
        position: { x: 80, y: 5 },
      })

      // Press F2 to trigger rename
      await window.keyboard.press(RENAME_KEY)

      // Wait for rename input to appear
      await expect(window.locator('text=Rename')).toBeVisible()
      const renameInput = window.locator('input:focus')
      await expect(renameInput).toBeVisible()

      // Type new name
      await window.keyboard.type('updated_marketing_out')

      // Confirm rename
      await window.keyboard.press('Enter')
      await window.waitForTimeout(1000)

      // Verify both definition and usage were renamed
      await expect(
        window.locator('text=WITH updated_marketing_out AS'),
      ).toBeVisible()
      await expect(
        window.locator('text=FROM updated_marketing_out'),
      ).toBeVisible()
    } finally {
      await close()
      await fs.remove(tempDir)
    }
  })

  test('Cancel CTE rename', async () => {
    const { window, close, tempDir } = await setupTestEnvironment()

    try {
      // Click on the CTE to rename
      await window.locator('text=current_marketing_outer').first().click()

      // Press F2 to trigger rename
      await window.keyboard.press(RENAME_KEY)

      // Wait for rename input to appear
      await expect(window.locator('text=Rename')).toBeVisible()
      const renameInput = window.locator('input:focus')
      await expect(renameInput).toBeVisible()

      // Type new name but cancel
      await window.keyboard.type('cancelled_name')
      await window.keyboard.press('Escape')

      // Wait for UI to update
      await window.waitForTimeout(500)

      // Verify CTE name was NOT changed
      await expect(
        window.locator('text=current_marketing_outer').first(),
      ).toBeVisible()
      await expect(window.locator('text=cancelled_name')).not.toBeVisible()
    } finally {
      await close()
      await fs.remove(tempDir)
    }
  })

  test('Rename CTE updates all references', async () => {
    const { window, close, tempDir } = await setupTestEnvironment()

    try {
      // Click on the CTE definition
      await window.locator('text=WITH current_marketing AS').click({
        position: { x: 100, y: 5 },
      })

      // Press F2 to trigger rename
      await window.keyboard.press(RENAME_KEY)
      // Wait for rename input to appear
      await expect(window.locator('text=Rename')).toBeVisible()
      const renameInput = window.locator('input:focus')
      await expect(renameInput).toBeVisible()

      // Type new name and confirm
      await window.keyboard.type('renamed_cte')
      await window.keyboard.press('Enter')

      // Click on the renamed CTE
      await window.locator('text=WITH renamed_cte AS').click({
        position: { x: 100, y: 5 },
      })

      // Find all references using keyboard shortcut
      await window.keyboard.press(FIND_ALL_REFERENCES_KEY)

      // Verify references panel shows all occurrences
      await window.waitForSelector('text=References')
      await expect(window.locator('text=customers.sql').first()).toBeVisible()
      await window.waitForSelector('text=WITH renamed_cte AS')
      await window.waitForSelector('text=renamed_cte.*')
      await window.waitForSelector('text=FROM renamed_cte')
      await window.waitForSelector('text=renamed_cte.customer_id != 100')
    } finally {
      await close()
      await fs.remove(tempDir)
    }
  })

  test('Rename CTE with preview', async () => {
    const { window, close, tempDir } = await setupTestEnvironment()

    try {
      // Click on the CTE to rename
      await window.locator('text=WITH current_marketing AS').click({
        position: { x: 100, y: 5 },
      })

      // Press F2 to trigger rename
      await window.keyboard.press(RENAME_KEY)
      await expect(window.locator('text=Rename')).toBeVisible()
      const renameInput = window.locator('input:focus')
      await expect(renameInput).toBeVisible()

      // Type new name
      await window.keyboard.type('preview_marketing')

      // Press Cmd+Enter (Meta+Enter) to preview changes
      await window.keyboard.press('Meta+Enter')

      // Verify preview UI is showing
      await expect(
        window.locator('text=Refactor Preview').first(),
      ).toBeVisible()
      await expect(window.locator('text=Apply').first()).toBeVisible()
      await expect(window.locator('text=Discard').first()).toBeVisible()

      // Verify the preview shows both old and new names
      await expect(
        window.locator('text=current_marketing').first(),
      ).toBeVisible()
      await expect(
        window.locator('text=preview_marketing').first(),
      ).toBeVisible()

      // Apply the changes
      await window.locator('text=Apply').click()

      // Verify the rename was applied
      await expect(
        window.locator('text=WITH preview_marketing AS'),
      ).toBeVisible()
    } finally {
      await close()
      await fs.remove(tempDir)
    }
  })
})
