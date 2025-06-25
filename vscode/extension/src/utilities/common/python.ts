// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { commands, Disposable, Event, EventEmitter, Uri } from 'vscode'
import { traceError, traceLog } from './log'
import { PythonExtension, ResolvedEnvironment } from '@vscode/python-extension'
import path from 'path'
import { err, ok, Result } from '@bus/result'
import * as vscode from 'vscode'

export interface IInterpreterDetails {
  path?: string[]
  resource?: Uri
  isVirtualEnvironment?: boolean
  binPath?: string
}

const onDidChangePythonInterpreterEvent =
  new EventEmitter<IInterpreterDetails>()

export const onDidChangePythonInterpreter: Event<IInterpreterDetails> =
  onDidChangePythonInterpreterEvent.event

let _api: PythonExtension | undefined

async function getPythonExtensionAPI(): Promise<PythonExtension | undefined> {
  if (_api) {
    return _api
  }
  _api = await PythonExtension.api()
  return _api
}

export async function initializePython(
  disposables: Disposable[],
): Promise<void> {
  try {
    const api = await getPythonExtensionAPI()

    if (api) {
      disposables.push(
        api.environments.onDidChangeActiveEnvironmentPath(async e => {
          const environment = await api.environments.resolveEnvironment(e.path)
          const isVirtualEnv = environment?.environment !== undefined
          // Get the directory of the Python executable for virtual environments
          const pythonDir = environment?.executable.uri
            ? path.dirname(environment.executable.uri.fsPath)
            : undefined

          onDidChangePythonInterpreterEvent.fire({
            path: [e.path],
            resource: e.resource?.uri,
            isVirtualEnvironment: isVirtualEnv,
            binPath: isVirtualEnv ? pythonDir : undefined,
          })
        }),
      )

      traceLog('Waiting for interpreter from python extension.')
      onDidChangePythonInterpreterEvent.fire(await getInterpreterDetails())
    }
  } catch (error) {
    traceError('Error initializing python: ', error)
  }
}

export async function resolveInterpreter(
  interpreter: string[],
): Promise<ResolvedEnvironment | undefined> {
  const api = await getPythonExtensionAPI()
  return api?.environments.resolveEnvironment(interpreter[0])
}

export async function getInterpreterDetails(
  resource?: Uri,
): Promise<IInterpreterDetails> {
  const api = await getPythonExtensionAPI()
  const environment = await api?.environments.resolveEnvironment(
    api?.environments.getActiveEnvironmentPath(resource),
  )
  if (environment?.executable.uri && checkVersion(environment)) {
    const isVirtualEnv = environment.environment !== undefined
    // Get the directory of the Python executable
    const pythonDir = path.dirname(environment?.executable.uri.fsPath)

    return {
      path: [environment?.executable.uri.fsPath],
      resource,
      isVirtualEnvironment: isVirtualEnv,
      // For virtual environments, we need to point directly to the bin directory
      // rather than constructing it from the environment folder
      binPath: isVirtualEnv ? pythonDir : undefined,
    }
  }
  return { path: undefined, resource }
}

export async function getDebuggerPath(): Promise<string | undefined> {
  const api = await getPythonExtensionAPI()
  return api?.debug.getDebuggerPackagePath()
}

export async function runPythonExtensionCommand(
  command: string,
  ...rest: any[]
) {
  await getPythonExtensionAPI()
  return await commands.executeCommand(command, ...rest)
}

export function checkVersion(
  resolved: ResolvedEnvironment | undefined,
): boolean {
  const version = resolved?.version
  if (version?.major === 3 && version?.minor >= 8) {
    return true
  }
  traceError(
    `Python version ${version?.major}.${version?.minor} is not supported.`,
  )
  traceError(`Selected python path: ${resolved?.executable.uri?.fsPath}`)
  traceError('Supported versions are 3.8 and above.')
  return false
}

/**
 * getPythonEnvVariables returns the environment variables for the current python interpreter.
 *
 * @returns The environment variables for the current python interpreter.
 */
export async function getPythonEnvVariables(): Promise<
  Result<Record<string, string>, string>
> {
  const api = await getPythonExtensionAPI()
  if (!api) {
    return err('Python extension API not found')
  }

  const workspaces = vscode.workspace.workspaceFolders
  if (!workspaces) {
    return ok({})
  }
  const out: Record<string, string> = {}
  for (const workspace of workspaces) {
    const envVariables = api.environments.getEnvironmentVariables(workspace.uri)
    if (envVariables) {
      for (const [key, value] of Object.entries(envVariables)) {
        if (value) {
          out[key] = value
        }
      }
    }
  }
  return ok(out)
}
