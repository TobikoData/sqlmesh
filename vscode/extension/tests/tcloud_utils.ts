import path from 'path'
import fs from 'fs-extra'

/**
 * Helper function to set up a pre-authenticated tcloud state
 */
export async function setupAuthenticatedState(tempDir: string): Promise<void> {
  const authStateFile = path.join(tempDir, '.tcloud_auth_state.json')
  const authState = {
    is_logged_in: true,
    id_token: {
      iss: 'https://mock.tobikodata.com',
      aud: 'mock-audience',
      sub: 'user-123',
      scope: 'openid email profile',
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 3600, // Valid for 1 hour
      email: 'test@example.com',
      name: 'Test User',
    },
  }
  await fs.writeJson(authStateFile, authState)
}

/**
 * Helper function to set the tcloud version for testing
 */
export async function setTcloudVersion(
  tempDir: string,
  version: string,
): Promise<void> {
  const versionStateFile = path.join(tempDir, '.tcloud_version_state.json')
  await fs.writeJson(versionStateFile, { version })
}
