import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

// Consistent keyboard shortcuts
const GO_TO_REFERENCES_KEY = 'Shift+F12'
const FIND_ALL_REFERENCES_KEY =
  process.platform === 'darwin' ? 'Alt+Shift+F12' : 'Ctrl+Shift+F12'

test.describe('CTE References', () => {
  let tempDir: string
  let window: any
  let close: () => Promise<void>

  test.beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    const vscode = await startVSCode(tempDir)
    window = vscode.window
    close = vscode.close

    // Common setup: navigate to customers.sql
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
  })

  test.afterEach(async () => {
    await close()
    fs.removeSync(tempDir)
  })

  test('Go to references from definition of CTE', async () => {
    // Click on the CTE definition "current_marketing_outer" at line 20 to position cursor
    await window.locator('text=current_marketing_outer').first().click()

    // Use keyboard shortcut to find all references
    await window.keyboard.press(GO_TO_REFERENCES_KEY)

    // Wait for the references to appear
    await window.waitForSelector('text=References')

    // Wait for reference panel to populate
    await window.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    // Verify that the customers.sql file is shown in results
    await expect(window.locator('text=customers.sql').first()).toBeVisible()

    // Check that both CTE definition and usage are listed in references
    await window.waitForSelector('text=References')
    await window.waitForSelector('text=WITH current_marketing_outer AS')
    await window.waitForSelector('text=FROM current_marketing_outer')
  })

  test('Go to references from usage of CTE', async () => {
    // Click on the CTE usage this time for "current_marketing_outer"
    await window.locator('text=FROM current_marketing_outer').click({
      position: { x: 80, y: 5 }, // Clicks on the usage rather than first which was definition
    })

    // Use keyboard shortcut to go to references
    await window.keyboard.press(GO_TO_REFERENCES_KEY)

    // Wait for the references to appear
    await window.waitForSelector('text=References')

    // Better assertions: wait for reference panel to populate
    await window.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    await window.waitForSelector('text=References')
    await window.waitForSelector('text=WITH current_marketing_outer AS')
    await window.waitForSelector('text=FROM current_marketing_outer')

    // Verify that the customers.sql file is shown in results
    await expect(window.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Go to references for nested CTE', async () => {
    // Click on the nested CTE "current_marketing"
    await window.locator('text=WITH current_marketing AS').click({
      position: { x: 100, y: 5 }, // Click on the CTE name part
    })

    // Use keyboard shortcut to find all references
    await window.keyboard.press(GO_TO_REFERENCES_KEY)

    // Wait for the references to appear
    await window.waitForSelector('text=References')

    // Wait for reference panel to populate
    await window.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    // Verify that the customers.sql file is shown in results
    await expect(window.locator('text=customers.sql').first()).toBeVisible()

    // Check that both CTE definition and usage are listed in references
    await window.waitForSelector('text=References')
    await window.waitForSelector('text=WITH current_marketing AS')
    await window.waitForSelector('text=FROM current_marketing')
  })

  test('Find all references for CTE', async () => {
    // Click on the CTE definition "current_marketing_outer"
    await window.locator('text=current_marketing_outer').first().click()

    // Use keyboard shortcut to find all references
    await window.keyboard.press(FIND_ALL_REFERENCES_KEY)

    // Verify references contains expected content
    await window.waitForSelector('text=References')
    await window.waitForSelector('text=WITH current_marketing_outer AS')
    await window.waitForSelector('text=FROM current_marketing_outer')

    // Verify that the customers.sql file is shown in results
    await expect(window.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Find all references from usage for CTE', async () => {
    // Click on the CTE usage of "current_marketing_outer" using last
    await window.locator('text=current_marketing_outer').last().click()

    // Use keyboard shortcut to find all references
    await window.keyboard.press(FIND_ALL_REFERENCES_KEY)

    // Verify references contains expected content
    await window.waitForSelector('text=References')
    await window.waitForSelector('text=WITH current_marketing_outer AS')
    await window.waitForSelector('text=FROM current_marketing_outer')

    // Verify that the customers.sql file is shown in results
    await expect(window.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Find all references for nested CTE', async () => {
    // Click on the nested CTE "current_marketing" at line 33
    // We need to be more specific to get the inner one
    await window.locator('text=WITH current_marketing AS').click({
      position: { x: 100, y: 5 }, // Click on the CTE name part
    })

    // Use keyboard shortcut to find all references
    await window.keyboard.press(FIND_ALL_REFERENCES_KEY)

    // Verify references contains expected content
    await window.waitForSelector('text=References')
    await window.waitForSelector('text=WITH current_marketing AS')
    await window.waitForSelector('text=FROM current_marketing')

    // Verify that the customers.sql file is shown in results
    await expect(window.locator('text=customers.sql').first()).toBeVisible()
  })
})
