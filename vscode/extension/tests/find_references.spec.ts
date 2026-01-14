import { test, expect, Page } from './fixtures'
import fs from 'fs-extra'
import {
  findAllReferences,
  goToReferences,
  openServerPage,
  SUSHI_SOURCE_PATH,
  waitForLoadedSQLMesh,
} from './utils'
import { createPythonInterpreterSettingsSpecifier } from './utils_code_server'

// Helper function to set up a test environment for model references
async function setupModelTestEnvironment(tempDir: string): Promise<void> {
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)
  await createPythonInterpreterSettingsSpecifier(tempDir)
}

// Helper function to navigate to models folder
async function navigateToModels(page: Page) {
  await page.waitForSelector('text=models')
  await page
    .getByRole('treeitem', { name: 'models', exact: true })
    .locator('a')
    .click()
}

// Helper function to navigate to audits folder
async function navigateToAudits(page: Page) {
  await page.waitForSelector('text=audits')
  await page
    .getByRole('treeitem', { name: 'audits', exact: true })
    .locator('a')
    .click()
}

// Helper function to open customers.sql and wait for SQLMesh context
async function openCustomersFile(page: Page) {
  await navigateToModels(page)
  await page
    .getByRole('treeitem', { name: 'customers.sql', exact: true })
    .locator('a')
    .click()
  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)
}

// Helper function to open top_waiters.sql and wait for SQLMesh context
async function openTopWaitersFile(page: Page) {
  await navigateToModels(page)
  await page
    .getByRole('treeitem', { name: 'top_waiters.sql', exact: true })
    .locator('a')
    .click()
  await page.waitForSelector('text=grain')
  await waitForLoadedSQLMesh(page)
}

test.describe('Model References', () => {
  test('Go to References (Shift+F12) for Model usage', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    // Open customers.sql which contains references to other models
    await openCustomersFile(page)

    // Step 4: Position cursor on the sushi.orders model reference in the SQL query
    await page.locator('text=sushi.orders').first().click()

    // Step 5: Trigger "Go to References" command using Shift+F12 keyboard shortcut
    await goToReferences(page)

    // Step 6: Wait for VSCode references panel to appear at the bottom
    await page.waitForSelector('text=References')

    // Step 7: Ensure references panel has populated with all usages of sushi.orders model
    await page.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 6
      },
      { timeout: 10000 },
    )

    // Step 8: Verify the references panel shows both SQL and Python files containing references
    const hasReferences = await page.evaluate(() => {
      const body = document.body.textContent || ''
      return (
        body.includes('References') &&
        (body.includes('.sql') || body.includes('.py'))
      )
    })

    expect(hasReferences).toBe(true)

    // Step 9: Find and click on the orders.py reference to navigate to the model definition
    let clickedReference = false

    const referenceItems = page.locator(
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
    await expect(page.locator('text=list(range(0, 100))')).toBeVisible()
  })

  test('Find All References (Alt+Shift+F12) for Model', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)

    // Open customers.sql which contains multiple model references
    await openCustomersFile(page)

    // Step 4: Click on sushi.orders model reference to position cursor
    await page.locator('text=sushi.orders').first().click()

    // Step 5: Trigger "Find All References" command using Alt+Shift+F12 (or +Shift+F12 on Windows/Linux)
    await findAllReferences(page)

    let clickedReference = false
    const referenceItems = page.locator(
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
    await expect(page.locator('text=import random')).toBeVisible()

    // Step 8: Click on the import statement to ensure file is fully loaded and interactive
    await page.locator('text=import random').first().click()

    // Step 9: Final verification that we're viewing the correct Python model file
    await expect(page.locator('text=list(range(0, 100))')).toBeVisible()
  })

  test('Go to References for Model from Audit', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    // Open assert_item_price_above_zero.sql audit file which references sushi.items model
    await navigateToAudits(page)
    await page
      .getByRole('treeitem', {
        name: 'assert_item_price_above_zero.sql',
        exact: true,
      })
      .locator('a')
      .click()

    // Wait for audit file to load and SQLMesh context to initialize
    await page.waitForSelector('text=standalone')
    await waitForLoadedSQLMesh(page)

    // Step 4: Click on sushi.items model reference in the audit query
    await page.locator('text=sushi.items').first().click()

    // Step 5: Trigger "Go to References" to find all places where sushi.items is used
    await goToReferences(page)

    // Step 6: Wait for VSCode references panel to appear
    await page.waitForSelector('text=References')

    // Step 7: Ensure references panel shows multiple files that reference sushi.items
    await page.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 4
      },
      { timeout: 10000 },
    )

    // Step 8: Verify references panel contains both audit and model files
    const hasReferences = await page.evaluate(() => {
      const body = document.body.textContent || ''
      return (
        body.includes('References') &&
        (body.includes('.sql') || body.includes('.py'))
      )
    })

    expect(hasReferences).toBe(true)

    // 9. Click on one of the references to navigate to it
    let clickedReference = false

    const referenceItems = page.locator(
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
    await expect(page.locator('text=LEFT JOIN')).toBeVisible()

    // Step 11: Click on LEFT JOIN to ensure file is interactive and verify content
    await page.locator('text=LEFT JOIN').first().click()
    await expect(
      page.locator('text=FROM sushi.order_items AS oi'),
    ).toBeVisible()
  })

  test('Find All Model References from Audit', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)

    // Open the audit file that validates item prices
    await navigateToAudits(page)
    await page
      .getByRole('treeitem', {
        name: 'assert_item_price_above_zero.sql',
        exact: true,
      })
      .locator('a')
      .click()

    // Ensure audit file and SQLMesh context are fully loaded
    await page.waitForSelector('text=standalone')
    await waitForLoadedSQLMesh(page)

    // Step 4: Position cursor on sushi.items model reference
    await page.locator('text=sushi.items').first().click()

    // Step 5: Use Find All References to see all occurrences across the project
    await findAllReferences(page)

    // Assert that the references panel shows the correct files
    await page.waitForSelector('text=References')
    await page.waitForSelector('text=customer_revenue_by_day.sql')
    await page.waitForSelector('text=items.py')
  })
})

test.describe('CTE References', () => {
  test('Go to references from definition of CTE', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    await openCustomersFile(page)

    // Click on the CTE definition "current_marketing_outer" at line 20 to position cursor
    await page.locator('text=current_marketing_outer').first().click()

    // Use keyboard shortcut to find all references
    await goToReferences(page)

    // Wait for the references to appear
    await page.waitForSelector('text=References')

    // Wait for reference panel to populate
    await page.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    // Verify that the customers.sql file is shown in results
    await expect(page.locator('text=customers.sql').first()).toBeVisible()

    // Check that both CTE definition and usage are listed in references
    await page.waitForSelector('text=References')
    await page.waitForSelector('text=WITH current_marketing_outer AS')
    await page.waitForSelector('text=FROM current_marketing_outer')
  })

  test('Go to references from usage of CTE', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)

    await openCustomersFile(page)

    // Click on the CTE usage this time for "current_marketing_outer"
    await page.locator('text=FROM current_marketing_outer').click({
      position: { x: 80, y: 5 }, // Clicks on the usage rather than first which was definition
    })

    // Use keyboard shortcut to go to references
    await goToReferences(page)

    // Wait for the references to appear
    await page.waitForSelector('text=References')

    // Better assertions: wait for reference panel to populate
    await page.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    await page.waitForSelector('text=References')
    await page.waitForSelector('text=WITH current_marketing_outer AS')
    await page.waitForSelector('text=FROM current_marketing_outer')

    // Verify that the customers.sql file is shown in results
    await expect(page.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Go to references for nested CTE', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)
    await openServerPage(page, tempDir, sharedCodeServer)

    await openCustomersFile(page)

    // Click on the nested CTE "current_marketing"
    await page.locator('text=WITH current_marketing AS').click({
      position: { x: 100, y: 5 }, // Click on the CTE name part
    })

    // Use keyboard shortcut to find all references
    await goToReferences(page)

    // Wait for the references to appear
    await page.waitForSelector('text=References')

    // Wait for reference panel to populate
    await page.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    // Verify that the customers.sql file is shown in results
    await expect(page.locator('text=customers.sql').first()).toBeVisible()

    // Check that both CTE definition and usage are listed in references
    await page.waitForSelector('text=References')
    await page.waitForSelector('text=WITH current_marketing AS')
    await page.waitForSelector('text=FROM current_marketing')
  })

  test('Find all references for CTE', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)
    await openCustomersFile(page)

    // Click on the CTE definition "current_marketing_outer"
    await page.locator('text=current_marketing_outer').first().click()

    // Use keyboard shortcut to find all references
    await findAllReferences(page)

    // Verify references contains expected content
    await page.waitForSelector('text=References')
    await page.waitForSelector('text=WITH current_marketing_outer AS')
    await page.waitForSelector('text=FROM current_marketing_outer')

    // Verify that the customers.sql file is shown in results
    await expect(page.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Find all references from usage for CTE', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)

    await openCustomersFile(page)

    // Click on the CTE usage of "current_marketing_outer" using last
    await page.locator('text=current_marketing_outer').last().click()

    // Use keyboard shortcut to find all references
    await findAllReferences(page)

    // Verify references contains expected content
    await page.waitForSelector('text=References')
    await page.waitForSelector('text=WITH current_marketing_outer AS')
    await page.waitForSelector('text=FROM current_marketing_outer')

    // Verify that the customers.sql file is shown in results
    await expect(page.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Find all references for nested CTE', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)
    await openCustomersFile(page)

    // Click on the nested CTE "current_marketing" at line 33
    // We need to be more specific to get the inner one
    await page.locator('text=WITH current_marketing AS').click({
      position: { x: 100, y: 5 }, // Click on the CTE name part
    })

    // Use keyboard shortcut to find all references
    await findAllReferences(page)

    // Verify references contains expected content
    await page.waitForSelector('text=References')
    await page.waitForSelector('text=WITH current_marketing AS')
    await page.waitForSelector('text=FROM current_marketing')

    // Verify that the customers.sql file is shown in results
    await expect(page.locator('text=customers.sql').first()).toBeVisible()
  })
})

test.describe('Macro References', () => {
  test('Go to References for @ADD_ONE macro', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)
    await openTopWaitersFile(page)

    // Click on the @ADD_ONE macro usage
    await page.locator('text=@ADD_ONE').first().click()

    // Use keyboard shortcut to find all references
    await goToReferences(page)

    // Wait for the references to appear
    await page.waitForSelector('text=References')

    // Wait for reference panel to populate
    await page.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    // Verify that both the definition and two usages are shown
    await expect(page.locator('text=utils.py').first()).toBeVisible()
    await expect(page.locator('text=top_waiters.sql').first()).toBeVisible()
    await expect(page.locator('text=customers.sql').first()).toBeVisible()
  })

  test('Find All References for @MULTIPLY macro', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)

    await openTopWaitersFile(page)

    // Click on the @MULTIPLY macro usage and then navigate to it
    await page.locator('text=@MULTIPLY').first().click()

    // Use keyboard shortcut to find all references
    await findAllReferences(page)

    // Verify references contains expected content
    await page.waitForSelector('text=References')

    // Verify that both utils.py (definition) and top_waiters.sql (usage) are shown
    await expect(page.locator('text=utils.py').first()).toBeVisible()
    await expect(page.locator('text=top_waiters.sql').first()).toBeVisible()

    // Click on the utils.py reference to navigate to the macro definition
    let clickedReference = false
    const referenceItems = page.locator(
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
    await expect(page.locator('text=def multiply')).toBeVisible()
    await page.locator('text=def multiply').first().click()

    // Verify navigation to utils.py by checking the import that appears there
    await expect(
      page.locator('text=from sqlmesh import SQL, macro'),
    ).toBeVisible()
  })

  test('Go to References for @SQL_LITERAL macro', async ({
    page,
    sharedCodeServer,
    tempDir,
  }) => {
    await setupModelTestEnvironment(tempDir)

    await openServerPage(page, tempDir, sharedCodeServer)
    await openTopWaitersFile(page)

    // Click on the @SQL_LITERAL macro usage
    await page.locator('text=@SQL_LITERAL').first().click()

    // Use keyboard shortcut to find references
    await goToReferences(page)

    // Wait for the references to appear
    await page.waitForSelector('text=References')

    // Wait for reference panel to populate
    await page.waitForFunction(
      () => {
        const referenceElements = document.querySelectorAll(
          '.reference-item, .monaco-list-row, .references-view .tree-row',
        )
        return referenceElements.length >= 2
      },
      { timeout: 5000 },
    )

    // Verify that references include both definition and usage
    const hasReferences = await page.evaluate(() => {
      const body = document.body.textContent || ''
      return (
        body.includes('References') &&
        body.includes('.py') &&
        body.includes('.sql')
      )
    })

    expect(hasReferences).toBe(true)

    await expect(page.locator('text=utils.py').first()).toBeVisible()
    await expect(page.locator('text=top_waiters.sql').first()).toBeVisible()
  })
})
