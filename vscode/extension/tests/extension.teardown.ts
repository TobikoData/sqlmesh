import { test as teardown } from '@playwright/test'
import path from 'path'
import fs from 'fs-extra'

teardown('cleanup extension', async () => {
  console.log('Cleaning up extension test setup...')

  const extensionDir = path.join(__dirname, '..')
  const testSetupDir = path.join(extensionDir, '.test_setup')

  // Clean up test setup directory
  if (fs.existsSync(testSetupDir)) {
    await fs.remove(testSetupDir)
    console.log('Test setup directory cleaned up')
  }
})
