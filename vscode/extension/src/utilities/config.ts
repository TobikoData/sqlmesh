import { workspace, WorkspaceFolder } from 'vscode'
import path from 'path'
import fs from 'fs'
import { Result, err, isErr, ok } from '@bus/result'
import { traceVerbose, traceInfo } from './common/log'
import { parse } from 'shell-quote'
import { z } from 'zod'

const sqlmeshConfigurationSchema = z.object({
  projectPaths: z.array(z.string()),
  lspEntryPoint: z.string(),
})

export type SqlmeshConfiguration = z.infer<typeof sqlmeshConfigurationSchema>

/**
 * Get the SQLMesh configuration from VS Code settings.
 *
 * @returns The SQLMesh configuration
 */
function getSqlmeshConfiguration(): SqlmeshConfiguration {
  const config = workspace.getConfiguration('sqlmesh')
  const projectPaths = config.get<string[]>('projectPaths', [])
  const lspEntryPoint = config.get<string>('lspEntrypoint', '')
  const parsed = sqlmeshConfigurationSchema.safeParse({
    projectPaths,
    lspEntryPoint,
  })
  if (!parsed.success) {
    throw new Error(
      `Invalid SQLMesh configuration: ${JSON.stringify(parsed.error)}`,
    )
  }
  return parsed.data
}

const stringsArray = z.array(z.string())

/**
 * Get the SQLMesh LSP entry point from VS Code settings. undefined if not set
 * it's expected to be a string in the format "command arg1 arg2 ...".
 */
export function getSqlmeshLspEntryPoint():
  | {
      entrypoint: string
      args: string[]
    }
  | undefined {
  const config = getSqlmeshConfiguration()
  if (config.lspEntryPoint === '') {
    return undefined
  }
  // Split the entry point into command and arguments
  const parts = parse(config.lspEntryPoint)
  const parsed = stringsArray.safeParse(parts)
  if (!parsed.success) {
    throw new Error(
      `Invalid lspEntrypoint configuration: ${config.lspEntryPoint}. Expected a
      string in the format "command arg1 arg2 ...".`,
    )
  }
  const entrypoint = parsed.data[0]
  const args = parsed.data.slice(1)
  return { entrypoint, args }
}

/**
 * Validate and resolve the project paths from configuration.
 * If no project path is configured, use the workspace folder.
 * If the project path is configured, it must be a directory that contains a SQLMesh project.
 *
 * @param workspaceFolder The current workspace folder
 * @returns A Result containing the resolved project paths or an error
 */
export function resolveProjectPath(workspaceFolder: WorkspaceFolder): Result<
  {
    projectPaths: string[] | undefined
    workspaceFolder: string
  },
  string
> {
  const config = getSqlmeshConfiguration()

  if (config.projectPaths.length === 0) {
    // If no project path is configured, use the workspace folder
    traceVerbose('No project path configured, using workspace folder')
    return ok({
      workspaceFolder: workspaceFolder.uri.fsPath,
      projectPaths: undefined,
    })
  }

  const resolvedPaths: string[] = []
  for (const projectPath of config.projectPaths) {
    const result = resolveSingleProjectPath(workspaceFolder, projectPath)
    if (isErr(result)) {
      return result
    }
    resolvedPaths.push(result.value)
  }
  return ok({
    projectPaths: resolvedPaths,
    workspaceFolder: workspaceFolder.uri.fsPath,
  })
}

function resolveSingleProjectPath(
  workspaceFolder: WorkspaceFolder,
  projectPath: string,
): Result<string, string> {
  let resolvedPath: string

  // Check if the path is absolute
  if (path.isAbsolute(projectPath)) {
    resolvedPath = projectPath
  } else {
    // Resolve relative path from workspace root
    resolvedPath = path.join(workspaceFolder.uri.fsPath, projectPath)
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
