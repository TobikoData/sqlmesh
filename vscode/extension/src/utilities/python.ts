import { getInterpreterDetails } from './common/python'
import { err, ok, Result } from '@bus/result'
import { traceInfo } from './common/log'
import { promisify } from 'util'
import { execFile } from 'child_process'

/** isPythonModuleInstallled returns true if the given python module is installed.
 *
 * @param moduleName - The name of the python module to check.
 * @returns True if the module is installed, false otherwise.
 */
export const isPythonModuleInstalled = async (
  moduleName: string,
): Promise<Result<boolean, string>> => {
  const interpreterDetails = await getInterpreterDetails()
  if (!interpreterDetails.path) {
    return err('No Python interpreter found')
  }
  const pythonPath = interpreterDetails.path[0]
  const checkScript = `
import sys
if sys.version_info >= (3, 12):
    from importlib import metadata
else:
    import importlib_metadata as metadata

try:
    metadata.version('${moduleName}')
    print("true")
except metadata.PackageNotFoundError:
    print("false")
`
  try {
    const execFileAsync = promisify(execFile)
    const { stdout } = await execFileAsync(pythonPath, ['-c', checkScript])
    const isInstalled = stdout.trim() === 'true'
    traceInfo(`${moduleName} is installed: ${isInstalled}`)

    return ok(stdout.trim() === 'true')
  } catch (error) {
    return err(`Failed to check tcloud installation: ${error}`)
  }
}
