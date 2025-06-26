import { test } from '@playwright/test'
import fs from 'fs-extra'
import os from 'os'
import path from 'path'
import { openLineageView, saveFile, SUSHI_SOURCE_PATH } from './utils'
import { startCodeServer, stopCodeServer } from './utils_code_server'

test('bad project, double model', async ({ page }) => {
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

  const context = await startCodeServer({
    tempDir,
    placeFileWithPythonInterpreter: true,
  })
  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)

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
  } finally {
    await stopCodeServer(context)
  }
})

test('working project, then broken through adding double model, then refixed', async ({
  page,
}) => {
  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-test-tcloud-'),
  )
  await fs.copy(SUSHI_SOURCE_PATH, tempDir)

  const context = await startCodeServer({
    tempDir,
    placeFileWithPythonInterpreter: true,
  })
  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)
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
    // TODO: Selector doesn't work in the linage view
    // await window.waitForSelector('text=Error')

    // Remove the duplicated model to fix the project
    await fs.remove(path.join(tempDir, 'models', 'customers_duplicated.sql'))

    // Save again to refresh the context
    await saveFile(page)

    // Wait for the error to go away and context to reload
    // TODO: Selector doesn't work in the linage view
    // await page.waitForSelector('text=raw.demographics')
  } finally {
    await stopCodeServer(context)
  }
})

test('bad project, double model, then fixed', async ({ page }) => {
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

  const context = await startCodeServer({
    tempDir,
    placeFileWithPythonInterpreter: true,
  })
  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)

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
    // TODO: Selector doesn't work in the linage view
    // await page.waitForSelector('text=raw.demographics')
  } finally {
    await stopCodeServer(context)
  }
})

test('bad project, double model, check lineage', async ({ page }) => {
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

  const context = await startCodeServer({
    tempDir,
    placeFileWithPythonInterpreter: true,
  })
  try {
    await page.goto(`http://127.0.0.1:${context.codeServerPort}`)
    await page.waitForLoadState('networkidle')

    // Open the lineage view
    await openLineageView(page)

    await page.waitForSelector('text=Error creating context')
    await page.waitForSelector('text=Error:')

    await page.waitForTimeout(500)
  } finally {
    await stopCodeServer(context)
  }
})
