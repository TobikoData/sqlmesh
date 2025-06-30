import { execSync } from 'child_process'
import path from 'path'
import fs from 'fs-extra'

async function globalSetup() {
  console.log('Setting up extension for Playwright tests...')

  const extensionDir = path.join(__dirname, '..')
  const testSetupDir = path.join(extensionDir, '.test_setup')
  const extensionsDir = path.join(testSetupDir, 'extensions')

  // Clean up any existing test setup directory
  await fs.remove(testSetupDir)
  await fs.ensureDir(extensionsDir)

  // Get the extension version from package.json
  const packageJson = JSON.parse(
    fs.readFileSync(path.join(extensionDir, 'package.json'), 'utf-8'),
  )
  const version = packageJson.version
  const extensionName = packageJson.name || 'sqlmesh'

  // Look for the specific version .vsix file
  const vsixFileName = `${extensionName}-${version}.vsix`
  const vsixPath = path.join(extensionDir, vsixFileName)

  if (!fs.existsSync(vsixPath)) {
    throw new Error(
      `Extension file ${vsixFileName} not found. Run "pnpm run vscode:package" first.`,
    )
  }

  console.log(`Installing extension: ${vsixFileName}`)

  // Create a temporary user data directory for the installation
  const tempUserDataDir = await fs.mkdtemp(
    path.join(require('os').tmpdir(), 'vscode-test-install-user-data-'),
  )

  try {
    execSync(
      `pnpm run code-server --user-data-dir "${tempUserDataDir}" --extensions-dir "${extensionsDir}" --install-extension "${vsixPath}"`,
      {
        stdio: 'inherit',
        cwd: extensionDir,
      },
    )
    console.log('Extension installed successfully to .test_setup/extensions')
  } finally {
    // Clean up temporary user data directory
    await fs.remove(tempUserDataDir)
  }
}

export default globalSetup
