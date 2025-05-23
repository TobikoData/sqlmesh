import { test, _electron as electron, expect, ElectronApplication, Page } from '@playwright/test';
import path from 'path';
import fs from 'fs-extra';
import os from 'os';

// Absolute path to the VS Code executable you downloaded in step 1.
const VS_CODE_EXE = fs.readJsonSync('.vscode-test/paths.json').executablePath;
// Where your extension lives on disk
const EXT_PATH = path.resolve(__dirname, '..');
// Where the sushi project lives which we copy from
const SUSHI_SOURCE_PATH = path.join(__dirname, '..', '..', '..', 'examples', 'sushi');

/**
 * Helper function to launch VS Code and test lineage with given project path config
 */
async function testLineageWithProjectPath(
  window: Page,
): Promise<void> {
    // Trigger lineage command
    await window.keyboard.press(process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P');
    await window.keyboard.type('Lineage: Focus On View');
    await window.keyboard.press('Enter');

    // Wait for "Loaded SQLmesh Context" text to appear
    const loadedContextText = window.locator('text=Loaded SQLMesh Context');
    await expect(loadedContextText.first()).toBeVisible({ timeout: 10_000 });
}

/**
 * Launch VS Code and return the window and a function to close the app.
 * @param workspaceDir The workspace directory to open.
 * @returns The window and a function to close the app.
 */
export const startVSCode = async (workspaceDir: string): Promise<{
  window: Page,
  close: () => Promise<void>,
}> => {
  const userDataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-user-data-'));
  const ciArgs = process.env.CI ? [
    '--disable-gpu',
    '--headless',
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--window-position=-10000,0',
  ] : [];
  const args = [
    ...ciArgs,
    `--extensionDevelopmentPath=${EXT_PATH}`,
    '--disable-workspace-trust',
    '--disable-telemetry',
    `--user-data-dir=${userDataDir}`,
    workspaceDir,
  ];
  const electronApp = await electron.launch({
    executablePath: VS_CODE_EXE,
    args,
  });
  const window = await electronApp.firstWindow();
  await window.waitForLoadState('domcontentloaded');
  await window.waitForLoadState('networkidle');
  await window.waitForTimeout(2_000);
  return { window, close: async () => {
    await electronApp.close();
    await fs.remove(userDataDir);
  } };
}

test('Lineage panel renders correctly - no project path config (default)', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'));
  await fs.copy(SUSHI_SOURCE_PATH, tempDir);
  try {
  const { window, close } = await startVSCode(tempDir);
  await testLineageWithProjectPath(window);
    await close();
  } finally { 
    await fs.remove(tempDir);
  }
});

test('Lineage panel renders correctly - relative project path', async () => {
  const workspaceDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-workspace-'));
  const projectDir = path.join(workspaceDir, 'projects', 'sushi');
  await fs.copy(SUSHI_SOURCE_PATH, projectDir);

  const settings = {
    "sqlmesh.projectPath": "./projects/sushi",
  };
  await fs.ensureDir(path.join(workspaceDir, '.vscode'));
  await fs.writeJson(path.join(workspaceDir, '.vscode', 'settings.json'), settings, { spaces: 2 });

  try {
    const { window, close } = await startVSCode(workspaceDir);
    await testLineageWithProjectPath(window);
    await close();
  } finally {
    await fs.remove(workspaceDir);
  }
});

test('Lineage panel renders correctly - absolute project path', async () => {
  const workspaceDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-workspace-'));
  const projectDir = path.join(workspaceDir, 'projects', 'sushi');
  await fs.ensureDir(path.join(workspaceDir, '.vscode'));
  await fs.copy(SUSHI_SOURCE_PATH, projectDir);
  await fs.ensureDir(path.join(workspaceDir, '.vscode'));
  const settings = {
    "sqlmesh.projectPath": projectDir,
  };
  await fs.writeJson(path.join(workspaceDir, '.vscode', 'settings.json'), settings, { spaces: 2 });

  try {
    const { window, close } = await startVSCode(workspaceDir);
    await testLineageWithProjectPath(window);
    await close();
  } finally {
    await fs.remove(workspaceDir);
  }
});


test("Lineage panel renders correctly - relative project outside of workspace", async () => {
  const tempFolder = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-workspace-'));
  const projectDir = path.join(tempFolder, 'projects', 'sushi');
  await fs.copy(SUSHI_SOURCE_PATH, projectDir);

  const workspaceDir = path.join(tempFolder, 'workspace');
  await fs.ensureDir(workspaceDir);

  const settings = {
    "sqlmesh.projectPath": "./../projects/sushi",
  };
  await fs.ensureDir(path.join(workspaceDir, '.vscode'));
  await fs.writeJson(path.join(workspaceDir, '.vscode', 'settings.json'), settings, { spaces: 2 });

  try {
    const { window, close } = await startVSCode(workspaceDir);
    await testLineageWithProjectPath(window);
    await close();
  } finally {
    await fs.remove(tempFolder);
  }
});

test("Lineage panel renders correctly - absolute path project outside of workspace", async () => {
  const tempFolder = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-workspace-'));
  const projectDir = path.join(tempFolder, 'projects', 'sushi');
  await fs.copy(SUSHI_SOURCE_PATH, projectDir);

  const workspaceDir = path.join(tempFolder, 'workspace');
  await fs.ensureDir(workspaceDir);

  const settings = {
    "sqlmesh.projectPath": projectDir,
  };
  await fs.ensureDir(path.join(workspaceDir, '.vscode'));
  await fs.writeJson(path.join(workspaceDir, '.vscode', 'settings.json'), settings, { spaces: 2 });

  try {
    const { window, close } = await startVSCode(workspaceDir);
    await testLineageWithProjectPath(window);
    await close();
  } finally {
    await fs.remove(tempFolder);
  }
});