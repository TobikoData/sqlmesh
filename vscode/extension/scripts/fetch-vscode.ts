import {
  downloadAndUnzipVSCode,
  resolveCliPathFromVSCodeExecutablePath,
} from '@vscode/test-electron'
import * as fs from 'fs-extra'
import * as path from 'path'
;(async () => {
  const vscPath = await downloadAndUnzipVSCode('stable') // one-time-only
  console.log('VS Code downloaded to:', vscPath)

  const cliPath = resolveCliPathFromVSCodeExecutablePath(vscPath) // optional
  console.log('CLI path:', cliPath)

  // Save paths to a JSON file for Playwright to use
  const pathsFile = path.join('.vscode-test', 'paths.json')
  await fs.ensureDir(path.dirname(pathsFile))
  await fs.writeJson(
    pathsFile,
    {
      executablePath: vscPath,
      cliPath: cliPath,
    },
    { spaces: 2 },
  )

  console.log('Paths saved to:', pathsFile)
})()
