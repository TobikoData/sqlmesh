import { spawn, ChildProcess, execSync } from 'child_process'
import path from 'path'
import fs from 'fs-extra'

export interface CodeServerContext {
  codeServerProcess: ChildProcess
  codeServerPort: number
  tempDir: string
}

/**
 * @param tempDir - The temporary directory to use for the code-server instance
 * @param placeFileWithPythonInterpreter - Whether to place a vscode/settings.json file in the temp directory that points to the python interpreter of the environmen the test is running in.
 * @returns The code-server context
 */
export async function startCodeServer(
  tempDir: string,
  placeFileWithPythonInterpreter: boolean = false,
): Promise<CodeServerContext> {
  // Find an available port
  const codeServerPort = Math.floor(Math.random() * 10000) + 50000

  // Create .vscode/settings.json with Python interpreter if requested
  if (placeFileWithPythonInterpreter) {
    const vscodeDir = path.join(tempDir, '.vscode')
    await fs.ensureDir(vscodeDir)

    // Get the current Python interpreter path
    const pythonPath = execSync('which python', {
      encoding: 'utf-8',
    }).trim()

    const settings = {
      'python.defaultInterpreterPath': path.join(
        __dirname,
        '..',
        '..',
        '..',
        '.venv',
        'bin',
        'python',
      ),
    }

    await fs.writeJson(path.join(vscodeDir, 'settings.json'), settings, {
      spaces: 2,
    })
    console.log(
      `Created .vscode/settings.json with Python interpreter: ${pythonPath}`,
    )
  }

  // Get the extension version from package.json
  const extensionDir = path.join(__dirname, '..')
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

  console.log(`Using extension: ${vsixFileName}`)

  // Install the extension first
  const extensionsDir = path.join(tempDir, 'extensions')
  console.log('Installing extension...')
  execSync(
    `pnpm run code-server --user-data-dir "${tempDir}" --extensions-dir "${extensionsDir}" --install-extension "${vsixPath}"`,
    { stdio: 'inherit' },
  )

  // Start code-server instance
  const codeServerProcess = spawn(
    'pnpm',
    [
      'run',
      'code-server',
      '--bind-addr',
      `127.0.0.1:${codeServerPort}`,
      '--auth',
      'none',
      '--disable-telemetry',
      '--disable-update-check',
      '--disable-workspace-trust',
      '--user-data-dir',
      tempDir,
      '--extensions-dir',
      extensionsDir,
      tempDir,
    ],
    {
      stdio: 'pipe',
      cwd: path.join(__dirname, '..'),
    },
  )

  // Wait for code-server to be ready
  await new Promise<void>((resolve, reject) => {
    let output = ''
    const timeout = setTimeout(() => {
      reject(new Error('Code-server failed to start within timeout'))
    }, 30000)

    codeServerProcess.stdout?.on('data', data => {
      output += data.toString()
      if (output.includes('HTTP server listening on')) {
        clearTimeout(timeout)
        resolve()
      }
    })

    codeServerProcess.stderr?.on('data', data => {
      console.error('Code-server stderr:', data.toString())
    })

    codeServerProcess.on('error', error => {
      clearTimeout(timeout)
      reject(error)
    })

    codeServerProcess.on('exit', code => {
      if (code !== 0) {
        clearTimeout(timeout)
        reject(new Error(`Code-server exited with code ${code}`))
      }
    })
  })

  return { codeServerProcess, codeServerPort, tempDir }
}

export async function stopCodeServer(
  context: CodeServerContext,
): Promise<void> {
  const { codeServerProcess, tempDir } = context

  // Clean up code-server process
  codeServerProcess.kill('SIGTERM')

  // Wait for process to exit
  await new Promise<void>(resolve => {
    codeServerProcess.on('exit', () => {
      resolve()
    })
    // Force kill after 5 seconds
    setTimeout(() => {
      if (!codeServerProcess.killed) {
        codeServerProcess.kill('SIGKILL')
      }
      resolve()
    }, 5000)
  })

  // Clean up temporary directory
  await fs.remove(tempDir)
}
