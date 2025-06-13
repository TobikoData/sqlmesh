import path from 'path'
import fs from 'fs-extra'
import os from 'os'
import { _electron as electron, Page } from '@playwright/test'

// Absolute path to the VS Code executable you downloaded in step 1.
export const VS_CODE_EXE = fs.readJsonSync(
  path.join(__dirname, '..', '.vscode-test', 'paths.json'),
).executablePath
// Where your extension lives on disk
export const EXT_PATH = path.resolve(__dirname, '..')
// Where the sushi project lives which we copy from
export const SUSHI_SOURCE_PATH = path.join(
  __dirname,
  '..',
  '..',
  '..',
  'examples',
  'sushi',
)

/**
 * Launch VS Code and return the window and a function to close the app.
 * @param workspaceDir The workspace directory to open.
 * @returns The window and a function to close the app.
 */
export const startVSCode = async (
  workspaceDir: string,
): Promise<{
  window: Page
  close: () => Promise<void>
}> => {
  const userDataDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'vscode-user-data-'),
  )
  const ciArgs = process.env.CI
    ? [
        '--disable-gpu',
        '--headless',
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--window-position=-10000,0',
      ]
    : []
  const args = [
    ...ciArgs,
    `--extensionDevelopmentPath=${EXT_PATH}`,
    '--disable-workspace-trust',
    '--disable-telemetry',
    '--install-extension=ms-python.python',
    `--user-data-dir=${userDataDir}`,
    workspaceDir,
  ]
  const electronApp = await electron.launch({
    executablePath: VS_CODE_EXE,
    args,
  })
  const window = await electronApp.firstWindow()
  await window.waitForLoadState('domcontentloaded')
  await window.waitForLoadState('networkidle')
  await clickExplorerTab(window)

  return {
    window,
    close: async () => {
      await electronApp.close()
      await fs.remove(userDataDir)
    },
  }
}

/**
 * Click on the Explorer tab in the VS Code activity bar if the Explorer tab is not already active.
 * This is necessary because the Explorer tab may not be visible if the user has not opened it yet.
 */
export const clickExplorerTab = async (page: Page): Promise<void> => {
  const isExplorerActive = await page.locator("text='Explorer'").isVisible()
  if (!isExplorerActive) {
    // Wait for the activity bar to be loaded
    await page.waitForSelector('.actions-container[role="tablist"]')

    // Click on the Explorer tab using the codicon class
    await page.click('.codicon-explorer-view-icon')

    // Wait a bit for the explorer view to activate
    await page.locator("text='Explorer'").waitFor({ state: 'visible' })
  }
}
