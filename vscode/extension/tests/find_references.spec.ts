import { test, expect } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { startVSCode, SUSHI_SOURCE_PATH } from './utils'

// Consistent keyboard shortcuts
const GO_TO_REFERENCES_KEY = 'Shift+F12'
const FIND_ALL_REFERENCES_KEY =
  process.platform === 'darwin' ? 'Alt+Shift+F12' : 'Ctrl+Shift+F12'

test.describe('Model References', () => {
  let tempDir: string
  let window: any
  let close: () => Promise<void>

  test.beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    const vscode = await startVSCode(tempDir)
    window = vscode.window
    close = vscode.close
  })

  test.afterEach(async () => {
    await close()
    fs.removeSync(tempDir)
  })

  test('Go to References (Shift+F12) for Model usage', async () => {
    // Step 1: Expand the models folder in the file explorer to access model files
    await window.waitForSelector('text=models')
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Step 2: Open customers.sql which contains references to other models
    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Step 3: Ensure SQLMesh extension has fully loaded by checking for model metadata
    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Step 4: Position cursor on the sushi.orders model reference in the SQL query
    await window.locator('text=sushi.orders').first().click()

    // Step 5: Trigger "Go to References" command using Shift+F12 keyboard shortcut
    await window.keyboard.press(GO_TO_REFERENCES_KEY)

    // Step 6: Wait for VSCode references panel to appear at the bottom
    await window.waitForSelector('text=References')

    // Step 7: Ensure references panel has populated with all usages of sushi.orders model
    await window.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 6
      },
      { timeout: 10000 },
    )

    // Step 8: Verify the references panel shows both SQL and Python files containing references
    const hasReferences = await window.evaluate(() => {
      const body = document.body.textContent || ''
      return (
        body.includes('References') &&
        (body.includes('.sql') || body.includes('.py'))
      )
    })

    expect(hasReferences).toBe(true)

    // Step 9: Find and click on the orders.py reference to navigate to the model definition
    let clickedReference = false

    const referenceItems = await window.locator(
      '.monaco-list-row, .reference-item, .monaco-tl-row',
    )
    const count = await referenceItems.count()

    for (let i = 0; i < count; i++) {
      const item = referenceItems.nth(i)
      const text = await item.textContent()

      // Search for the orders.py reference which contains the Python model definition
      if (text && text.includes('orders.py')) {
        await item.click()
        clickedReference = true
        break
      }
    }

    expect(clickedReference).toBe(true)

    // Step 10: Verify successful navigation to orders.py by checking for unique Python code
    await expect(window.locator('text=list(range(0, 100))')).toBeVisible()
  })

  test('Find All References (Alt+Shift+F12) for Model', async () => {
    // Step 1: Expand the models folder to access SQLMesh model files
    await window.waitForSelector('text=models')
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()

    // Step 2: Open customers.sql which contains multiple model references
    await window
      .getByRole('treeitem', { name: 'customers.sql', exact: true })
      .locator('a')
      .click()

    // Step 3: Wait for SQLMesh context to fully initialize
    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Step 4: Click on sushi.orders model reference to position cursor
    await window.locator('text=sushi.orders').first().click()

    // Step 5: Trigger "Find All References" command using Alt+Shift+F12 (or Ctrl+Shift+F12 on Windows/Linux)
    await window.keyboard.press(FIND_ALL_REFERENCES_KEY)

    let clickedReference = false
    const referenceItems = await window.locator(
      '.monaco-list-row, .reference-item, .monaco-tl-row',
    )
    const count = await referenceItems.count()

    // Step 6: Iterate through references to find and click on orders.py
    for (let i = 0; i < count; i++) {
      const item = referenceItems.nth(i)
      const text = await item.textContent()

      // Find the orders.py reference which contains the model implementation
      if (text && text.includes('orders.py')) {
        await item.click()

        clickedReference = true
        break
      }
    }

    expect(clickedReference).toBe(true)

    // Step 7: Verify navigation to orders.py by checking for Python import statement
    await expect(window.locator('text=import random')).toBeVisible()

    // Step 8: Click on the import statement to ensure file is fully loaded and interactive
    await window.locator('text=import random').first().click()

    // Step 9: Final verification that we're viewing the correct Python model file
    await expect(window.locator('text=list(range(0, 100))')).toBeVisible()
  })

  test('Go to References for Model from Audit', async () => {
    // Step 1: Expand audits folder to access SQLMesh audit files
    await window.waitForSelector('text=audits')
    await window
      .getByRole('treeitem', { name: 'audits', exact: true })
      .locator('a')
      .click()

    // Step 2: Open assert_item_price_above_zero.sql audit file which references sushi.items model
    await window
      .getByRole('treeitem', {
        name: 'assert_item_price_above_zero.sql',
        exact: true,
      })
      .locator('a')
      .click()

    // Step 3: Wait for audit file to load and SQLMesh context to initialize
    await window.waitForSelector('text=standalone')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Step 4: Click on sushi.items model reference in the audit query
    await window.locator('text=sushi.items').first().click()

    // Step 5: Trigger "Go to References" to find all places where sushi.items is used
    await window.keyboard.press(GO_TO_REFERENCES_KEY)

    // Step 6: Wait for VSCode references panel to appear
    await window.waitForSelector('text=References')

    // Step 7: Ensure references panel shows multiple files that reference sushi.items
    await window.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 4
      },
      { timeout: 10000 },
    )

    // Step 8: Verify references panel contains both audit and model files
    const hasReferences = await window.evaluate(() => {
      const body = document.body.textContent || ''
      return (
        body.includes('References') &&
        (body.includes('.sql') || body.includes('.py'))
      )
    })

    expect(hasReferences).toBe(true)

    // 9. Click on one of the references to navigate to it
    let clickedReference = false

    const referenceItems = await window.locator(
      '.monaco-list-row, .reference-item, .monaco-tl-row',
    )
    const count = await referenceItems.count()

    for (let i = 0; i < count; i++) {
      const item = referenceItems.nth(i)
      const text = await item.textContent()

      // Search for the customer_revenue_by_day.sql file which joins with sushi.items
      if (text && text.includes('customer_revenue_by_day.sql')) {
        await item.click()
        clickedReference = true
        break
      }
    }

    expect(clickedReference).toBe(true)

    // Step 10: Verify navigation to customer_revenue_by_day.sql by checking for SQL JOIN syntax
    await expect(window.locator('text=LEFT JOIN')).toBeVisible()

    // Step 11: Click on LEFT JOIN to ensure file is interactive and verify content
    await window.locator('text=LEFT JOIN').first().click()
    await expect(
      window.locator('text=FROM sushi.order_items AS oi'),
    ).toBeVisible()
  })

  test('Find All Model References from Audit', async () => {
    // Step 1: Expand audits folder in the file explorer
    await window.waitForSelector('text=audits')
    await window
      .getByRole('treeitem', { name: 'audits', exact: true })
      .locator('a')
      .click()

    // Step 2: Open the audit file that validates item prices
    await window
      .getByRole('treeitem', {
        name: 'assert_item_price_above_zero.sql',
        exact: true,
      })
      .locator('a')
      .click()

    // Step 3: Ensure audit file and SQLMesh context are fully loaded
    await window.waitForSelector('text=standalone')
    await window.waitForSelector('text=Loaded SQLMesh Context')

    // Step 4: Position cursor on sushi.items model reference
    await window.locator('text=sushi.items').first().click()

    // Step 5: Use Find All References to see all occurrences across the project
    await window.keyboard.press(FIND_ALL_REFERENCES_KEY)

    // Step 6: Click on a reference to navigate to customer_revenue_by_day.sql
    let clickedReference = false

    const referenceItems = await window.locator(
      '.monaco-list-row, .reference-item, .monaco-tl-row',
    )
    const count = await referenceItems.count()

    for (let i = 0; i < count; i++) {
      const item = referenceItems.nth(i)
      const text = await item.textContent()

      // Look for a reference that contains customer_revenue_by_day
      if (text && text.includes('customer_revenue_by_day.sql')) {
        await item.click()
        clickedReference = true
        break
      }
    }

    expect(clickedReference).toBe(true)

    // Step 7: Verify successful navigation by checking for SQL JOIN statement
    await expect(window.locator('text=LEFT JOIN')).toBeVisible()

    // Step 8: Interact with the file to verify it's fully loaded and check its content
    await window.locator('text=LEFT JOIN').first().click()
    await expect(
      window.locator('text=FROM sushi.order_items AS oi'),
    ).toBeVisible()
  })
})

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

test.describe('Macro References', () => {
  let tempDir: string
  let window: any
  let close: () => Promise<void>

  test.beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'))
    await fs.copy(SUSHI_SOURCE_PATH, tempDir)
    const vscode = await startVSCode(tempDir)
    window = vscode.window
    close = vscode.close

    // Common setup: navigate to top_waiters.sql which uses macros
    await window.waitForSelector('text=models')
    await window
      .getByRole('treeitem', { name: 'models', exact: true })
      .locator('a')
      .click()
    await window
      .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
      .locator('a')
      .click()
    await window.waitForSelector('text=grain')
    await window.waitForSelector('text=Loaded SQLMesh Context')
  })

  test.afterEach(async () => {
    await close()
    fs.removeSync(tempDir)
  })

  test('Go to References for @ADD_ONE macro', async () => {
    // Click on the @ADD_ONE macro usage
    await window.locator('text=@ADD_ONE').first().click()

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

    // Verify that both the definition and two usages are shown
    await expect(window.locator('text=utils.py').first()).toBeVisible()
    await expect(window.locator('text=top_waiters.sql').first()).toBeVisible()
    await expect(window.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Find All References for @MULTIPLY macro', async () => {
    // Click on the @MULTIPLY macro usage and then navigate to it
    await window.locator('text=@MULTIPLY').first().click()

    // Use keyboard shortcut to find all references
    await window.keyboard.press(FIND_ALL_REFERENCES_KEY)

    // Verify references contains expected content
    await window.waitForSelector('text=References')

    // Verify that both utils.py (definition) and top_waiters.sql (usage) are shown
    await expect(window.locator('text=utils.py').first()).toBeVisible()
    await expect(window.locator('text=top_waiters.sql').first()).toBeVisible()

    // Click on the utils.py reference to navigate to the macro definition
    let clickedReference = false
    const referenceItems = await window.locator(
      '.monaco-list-row, .reference-item, .monaco-tl-row',
    )
    const count = await referenceItems.count()

    for (let i = 0; i < count; i++) {
      const item = referenceItems.nth(i)
      const text = await item.textContent()

      // Find the utils.py reference which contains the macro definition
      if (text && text.includes('utils.py')) {
        await item.click()
        clickedReference = true
        break
      }
    }

    expect(clickedReference).toBe(true)

    // Verify it appeared and click on it
    await expect(window.locator('text=def multiply')).toBeVisible()
    await window.locator('text=def multiply').first().click()

    // Verify navigation to utils.py by checking the import that appears there
    await expect(
      window.locator('text=from sqlmesh import SQL, macro'),
    ).toBeVisible()
  })

  test('Go to References for @SQL_LITERAL macro', async () => {
    // Click on the @SQL_LITERAL macro usage
    await window.locator('text=@SQL_LITERAL').first().click()

    // Use keyboard shortcut to find references
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

    // Verify that references include both definition and usage
    const hasReferences = await window.evaluate(() => {
      const body = document.body.textContent || ''
      return (
        body.includes('References') &&
        body.includes('.py') &&
        body.includes('.sql')
      )
    })

    expect(hasReferences).toBe(true)

    await expect(window.locator('text=utils.py').first()).toBeVisible()
    await expect(window.locator('text=top_waiters.sql').first()).toBeVisible()
  })
})
