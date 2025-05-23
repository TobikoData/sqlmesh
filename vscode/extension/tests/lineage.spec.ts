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
  workspaceDir: string,
  projectDir: string,
  projectPathConfig?: string
): Promise<void> {
  const ciArgs = process.env.CI ? [
    '--disable-gpu',
    '--headless',
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--window-position=-10000,0',
  ] : [];
  
  const userDataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-user-data-'));
  
  try {
    // If projectPathConfig is provided, create .vscode/settings.json in the workspace
    if (projectPathConfig !== undefined) {
      const vscodeDir = path.join(workspaceDir, '.vscode');
      await fs.ensureDir(vscodeDir);
      const settings = {
        "sqlmesh.projectPath": projectPathConfig
      };
      await fs.writeJson(path.join(vscodeDir, 'settings.json'), settings, { spaces: 2 });
    }
    
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
    
    // Wait a bit for the extension to fully initialize with the settings
    await window.waitForTimeout(2000);

    // Trigger lineage command
    await window.keyboard.press(process.platform === 'darwin' ? 'Meta+Shift+P' : 'Control+Shift+P');
    await window.keyboard.type('Lineage: Focus On View');
    await window.keyboard.press('Enter');

    // Wait for "Loaded SQLmesh Context" text to appear
    const loadedContextText = window.locator('text=Loaded SQLMesh Context');
    await expect(loadedContextText.first()).toBeVisible({ timeout: 15000 });

    await electronApp.close();
  } finally {
    await fs.remove(userDataDir);
  }
}

export const startVSCode = async (workspaceDir: string) => {
  
}

test('Lineage panel renders correctly - no project path config (default)', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-'));
  await fs.copy(SUSHI_SOURCE_PATH, tempDir);

  try {
    await testLineageWithProjectPath(tempDir, tempDir);
  } finally {
    await fs.remove(tempDir);
  }
});

test('Lineage panel renders correctly - relative project path', async () => {
  // Create workspace directory with subdirectory containing the project
  const workspaceDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-workspace-'));
  const projectSubdir = path.join(workspaceDir, 'projects', 'sushi');
  await fs.ensureDir(path.dirname(projectSubdir));
  await fs.copy(SUSHI_SOURCE_PATH, projectSubdir);

  try {
    // Test with relative path
    await testLineageWithProjectPath(workspaceDir, projectSubdir, 'projects/sushi');
  } finally {
    await fs.remove(workspaceDir);
  }
});

test('Lineage panel renders correctly - absolute project path', async () => {
  // Create workspace directory
  const workspaceDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-workspace-'));
  
  // Create project directory outside workspace
  const projectDir = await fs.mkdtemp(path.join(os.tmpdir(), 'vscode-test-sushi-project-'));
  await fs.copy(SUSHI_SOURCE_PATH, projectDir);

  try {
    // Test with absolute path
    await testLineageWithProjectPath(workspaceDir, projectDir, projectDir);
  } finally {
    await fs.remove(workspaceDir);
    await fs.remove(projectDir);
  }
});
