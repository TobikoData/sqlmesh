import { workspace, WorkspaceFolder } from 'vscode'
import path from 'path'
import fs from 'fs'
import { Result, err, ok } from '@bus/result'
import { traceVerbose, traceInfo } from './common/log'

export interface SqlmeshConfiguration {
  projectPath: string
}

/**
 * Get the SQLMesh configuration from VS Code settings.
 *
 * @returns The SQLMesh configuration
 */
export function getSqlmeshConfiguration(): SqlmeshConfiguration {
  const config = workspace.getConfiguration('sqlmesh')
  const projectPath = config.get<string>('projectPath', '')
  return {
    projectPath,
  }
}

/**
 * Validate and resolve the project path from configuration.
 * If no project path is configured, use the workspace folder.
 * If the project path is configured, it must be a directory that contains a SQLMesh project.
 *
 * @param workspaceFolder The current workspace folder
 * @returns A Result containing the resolved project path or an error
 */
export function resolveProjectPath(
  workspaceFolder: WorkspaceFolder,
): Result<string, string> {
  const config = getSqlmeshConfiguration()

  if (!config.projectPath) {
    // If no project path is configured, use the workspace folder
    traceVerbose('No project path configured, using workspace folder')
    return ok(workspaceFolder.uri.fsPath)
  }
  let resolvedPath: string

  // Check if the path is absolute
  if (path.isAbsolute(config.projectPath)) {
    resolvedPath = config.projectPath
  } else {
    // Resolve relative path from workspace root
    resolvedPath = path.join(workspaceFolder.uri.fsPath, config.projectPath)
  }

  // Normalize the path
  resolvedPath = path.normalize(resolvedPath)

  // Validate that the path exists
  if (!fs.existsSync(resolvedPath)) {
    return err(`Configured project path does not exist: ${resolvedPath}`)
  }

  // Validate that it's a directory
  const stats = fs.statSync(resolvedPath)
  if (!stats.isDirectory()) {
    return err(`Configured project path is not a directory: ${resolvedPath}`)
  }

  // Check if it contains SQLMesh project files (config.yaml, config.yml, or config.py)
  const configFiles = ['config.yaml', 'config.yml', 'config.py']
  const hasConfigFile = configFiles.some(file =>
    fs.existsSync(path.join(resolvedPath, file)),
  )
  if (!hasConfigFile) {
    traceInfo(`Warning: No SQLMesh configuration file found in ${resolvedPath}`)
  }

  traceVerbose(`Using project path: ${resolvedPath}`)
  return ok(resolvedPath)
}
