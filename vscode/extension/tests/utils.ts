import path from 'path'
import { Page } from '@playwright/test'
import { exec } from 'child_process'
import { promisify } from 'util'

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
export const REPO_ROOT = path.join(__dirname, '..', '..', '..')

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

const execAsync = promisify(exec)

export interface PythonEnvironment {
  pythonPath: string
  pipPath: string
}

/**
 * Create a virtual environment in the given directory.
 * @param venvDir The directory to create the virtual environment in.
 */
export const createVirtualEnvironment = async (
  venvDir: string,
): Promise<PythonEnvironment> => {
  const pythonCmd = process.platform === 'win32' ? 'python' : 'python3'
  const { stderr } = await execAsync(`${pythonCmd} -m venv "${venvDir}"`)
  if (stderr && !stderr.includes('WARNING')) {
    throw new Error(`Failed to create venv: ${stderr}`)
  }
  // Get paths
  const isWindows = process.platform === 'win32'
  const binDir = path.join(venvDir, isWindows ? 'Scripts' : 'bin')
  const pythonPath = path.join(binDir, isWindows ? 'python.exe' : 'python')
  const pipPath = path.join(binDir, isWindows ? 'pip.exe' : 'pip')

  return {
    pythonPath,
    pipPath,
  }
}

/**
 * Install packages in the given virtual environment.
 * @param pythonDetails The Python environment to use.
 * @param packagePaths The paths to the packages to install (string[]).
 */
export const pipInstall = async (
  pythonDetails: PythonEnvironment,
  packagePaths: string[],
): Promise<void> => {
  const { pipPath } = pythonDetails
  const execString = `"${pipPath}" install -e "${packagePaths.join('" -e "')}"`
  const { stderr } = await execAsync(execString)
  if (stderr && !stderr.includes('WARNING') && !stderr.includes('notice')) {
    throw new Error(`Failed to install package: ${stderr}`)
  }
}

/**
 * Open the lineage view in the given window.
 */
export const openLineageView = async (window: Page): Promise<void> => {
  await window.keyboard.press(
    process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
  )
  await window.keyboard.type('Lineage: Focus On View')
  await window.keyboard.press('Enter')
}

/**
 * Restart the SQLMesh servers
 */
export const restartSqlmeshServers = async (window: Page): Promise<void> => {
  await window.keyboard.press(
    process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
  )
  await window.keyboard.type('Restart SQLMesh servers')
  await window.keyboard.press('Enter')
}

/**
 * Open the vscode command palette and run the given command.
 * @param window The window to run the command in.
 * @param command The command to run.
 */
export const runCommand = async (
  window: Page,
  command: string,
): Promise<void> => {
  await window.keyboard.press(
    process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P',
  )
  await window.keyboard.type(command)
  await window.keyboard.press('Enter')
}

/**
 * Open the vscode code file picker and select the given file.
 * @param window The window to run the command in.
 * @param filePath The path to the file to select.
 */
export const openFile = async (window: Page, file: string): Promise<void> => {
  await window.keyboard.press(
    process.platform === 'darwin' ? 'Meta+P' : 'Control+P',
  )
  await window.waitForTimeout(100)
  await window.keyboard.type(file)
  await window.waitForTimeout(100)
  await window.keyboard.press('Enter')
  await window.waitForTimeout(100)
}
