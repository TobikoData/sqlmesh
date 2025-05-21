# E2E Testing with Playwright

This directory contains end-to-end tests for the SQLMesh VS Code extension using Playwright.

## Setup

1. **Install dependencies:**
   ```bash
   pnpm install
   ```

2. **Download VS Code executable (one-time setup):**
   ```bash
   pnpm run fetch-vscode
   ```
   
   This downloads VS Code and caches it in `.vscode-test/` directory. The paths are saved to `.vscode-test/paths.json` for Playwright to use.

3. **Install Playwright browsers:**
   ```bash
   npx playwright install
   ```

## Running Tests

- **Run all E2E tests:**
  ```bash
  pnpm run test:e2e
  ```

- **Run tests with UI (interactive):**
  ```bash
  pnpm run test:e2e:ui
  ```

- **Run tests in headed mode (visible browser):**
  ```bash
  pnpm run test:e2e:headed
  ```

## Test Structure

- `scripts/fetch-vscode.ts` - Downloads and caches VS Code executable
- `playwright.config.ts` - Playwright configuration for Electron testing
- `tests/lineage.spec.ts` - E2E tests for lineage functionality

## How It Works

1. **VS Code as Electron app:** Playwright launches VS Code as an Electron application with the extension loaded in development mode
2. **Extension isolation:** Each test runs with a fresh user data directory (`/tmp/vscode-test`)
3. **Webview testing:** Tests can interact with webview content using frame locators
4. **Visual regression:** Screenshots are captured and compared for pixel-perfect testing

## CI/CD

- The `.vscode-test` directory should be cached in CI to avoid re-downloading VS Code
- Tests run in headless mode by default in CI environments
- Screenshots are stored as test artifacts for comparison

## Adding New Tests

Create new test files in the `tests/` directory following the pattern:

```typescript
import { test, expect, _electron as electron } from '@playwright/test';
import path from 'path';
import fs from 'fs-extra';

const VS_CODE_EXE = fs.readJsonSync('.vscode-test/paths.json').executablePath;
const EXT_PATH = path.join(__dirname, '..');

test('my new test', async () => {
  const electronApp = await electron.launch({
    executablePath: VS_CODE_EXE,
    args: [
      `--extensionDevelopmentPath=${EXT_PATH}`,
      '--disable-workspace-trust',
      '--disable-telemetry',
      '--user-data-dir=/tmp/vscode-test',
    ],
  });

  const window = await electronApp.firstWindow();
  
  // Your test logic here...
  
  await electronApp.close();
});
```