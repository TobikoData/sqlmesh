import { test, _electron as electron, expect } from '@playwright/test';
import path from 'path';
import fs from 'fs-extra';
import os from 'os';

// Absolute path to the VS Code executable you downloaded in step 1.
const VS_CODE_EXE = fs.readJsonSync('.vscode-test/paths.json').executablePath;
// Where your extension lives on disk
const EXT_PATH = path.resolve(__dirname, '..');
// Where the sushi project lives which we copy from
const SUSHI_SOURCE_PATH = path.join(__dirname, '..', '..', '..', 'examples', 'sushi');

test('Lineage panel renders correctly', async () => {
  // Create a temporary directory and copy sushi example into it
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'));
  await fs.copy(SUSHI_SOURCE_PATH, tempDir);

  try {
    const ciArgs = process.env.CI ? [
      '--disable-gpu',
      '--headless',
      '--no-sandbox',
      '--disable-dev-shm-usage',  // Prevents memory issues in Docker/CI
      '--window-position=-10000,0', // Ensures window is off-screen
    ] : [];
    const args = [
      ...ciArgs,
      `--extensionDevelopmentPath=${EXT_PATH}`,
      '--disable-workspace-trust',          // no modal prompt
      '--disable-telemetry',
      '--user-data-dir=/tmp/vscode-test',   // throwaway profile
      `${tempDir}`,  // Use the temporary directory instead of PROJECT_PATH
    ];
    const electronApp = await electron.launch({
      executablePath: VS_CODE_EXE,
      args,
    });

    // ➋ Grab the first window that appears (the Workbench)
    const window = await electronApp.firstWindow();

    // Wait for VS Code to be ready
    await window.waitForLoadState('domcontentloaded');
    await window.waitForLoadState('networkidle');

    // ➌ Trigger our command exactly like a user would
    await window.keyboard.press(process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P');
    await window.keyboard.type('Lineage: Focus On View');
    await window.keyboard.press('Enter');

    // Wait for "Loaded SQLmesh Context" text to appear
    const loadedContextText = window.locator('text=Loaded SQLMesh Context');
    await expect(loadedContextText.first()).toBeVisible({ timeout: 10000 });

    await electronApp.close();
  } finally {
    // Clean up the temporary directory
    await fs.remove(tempDir);
  }
});
