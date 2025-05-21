import { test, _electron as electron, expect } from '@playwright/test';
import path from 'path';
import fs from 'fs-extra';

// Absolute path to the VS Code executable you downloaded in step 1.
const VS_CODE_EXE = fs.readJsonSync('.vscode-test/paths.json').executablePath;
// Where your extension lives on disk
const EXT_PATH = path.join(__dirname, '..');
// Where the sushi project lives which we open in the webview
const PROJECT_PATH = path.join(__dirname, '..', '..', '..', 'examples', 'sushi');

test('Lineage panel renders correctly', async () => {
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
    `${PROJECT_PATH}`,
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

  await expect(window.locator("text=lineage").first()).toBeVisible();

  // ➌ Trigger our command exactly like a user would
  await window.keyboard.press(process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P');
  await window.keyboard.type('Lineage: Focus On View');
  await window.keyboard.press('Enter');

  // Wait for "Loaded SQLmesh Context" text to appear
  const loadedContextText = window.locator('text=Loaded SQLmesh Context');
  await expect(loadedContextText).toBeVisible({ timeout: 10000 });

  await electronApp.close();
});
