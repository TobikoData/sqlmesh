import { test as setup } from '@playwright/test'
import { execSync } from 'child_process'
import path from 'path'
import fs from 'fs-extra'
import { createHash } from 'crypto'
import { tmpdir } from 'os'

setup('prepare extension', async () => {
  console.log('Setting up extension for Playwright tests...')

  const extensionDir = path.join(__dirname, '..')
  const testSetupDir = path.join(extensionDir, '.test_setup')
  const extensionsDir = path.join(testSetupDir, 'extensions')

  // Clean up any existing test setup directory

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

  // Create a temporary user data directory for the installation
  const tempUserDataDir = await fs.mkdtemp(
    path.join(tmpdir(), 'vscode-test-install-user-data-'),
  )

  try {
    // Check if in .test_setup there is a extension hash file which contains the hash of the extension
    // If it does, check if the hash is the same as the hash of the extension in the vsix file
    // If it is, skip the installation
    // If it is not, remove the extension hash file and install the extension
    const extensionHashFile = path.join(testSetupDir, 'extension-hash.txt')
    console.log('extensionHashFile', extensionHashFile)
    if (fs.existsSync(extensionHashFile)) {
      const extensionHash = fs.readFileSync(extensionHashFile, 'utf-8')
      const vsixHash = await hashFile(vsixPath)
      if (extensionHash === vsixHash) {
        console.log('Extension already installed')
        return
      }
    }

    await fs.remove(testSetupDir)
    await fs.ensureDir(testSetupDir)
    await fs.ensureDir(extensionsDir)

    console.log(`Installing extension: ${vsixFileName}`)
    execSync(
      `pnpm run code-server --user-data-dir "${tempUserDataDir}" --extensions-dir "${extensionsDir}" --install-extension "${vsixPath}"`,
      {
        stdio: 'inherit',
        cwd: extensionDir,
      },
    )

    // Write the hash of the extension to the extension hash file
    const extensionHash = await hashFile(vsixPath)
    await fs.writeFile(extensionHashFile, extensionHash)
  } finally {
    // Clean up temporary user data directory
    await fs.remove(tempUserDataDir)
  }
})

async function hashFile(filePath: string): Promise<string> {
  const fileBuffer = await fs.readFile(filePath)
  return createHash('sha256').update(fileBuffer).digest('hex')
}
