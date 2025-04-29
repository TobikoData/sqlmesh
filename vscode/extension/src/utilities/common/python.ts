// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import { commands, Disposable, Event, EventEmitter, Uri } from 'vscode'
import { traceError, traceLog } from './log'
import { PythonExtension, ResolvedEnvironment } from '@vscode/python-extension'
import path from 'path'

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
          const binPath = isVirtualEnv
            ? environment?.environment?.folderUri.fsPath
            : undefined

          onDidChangePythonInterpreterEvent.fire({
            path: [e.path],
            resource: e.resource?.uri,
            isVirtualEnvironment: isVirtualEnv,
            binPath,
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
    const binPath = isVirtualEnv
      ? environment.environment?.folderUri.fsPath
      : undefined

    return {
      path: [environment?.executable.uri.fsPath],
      resource,
      isVirtualEnvironment: isVirtualEnv,
      binPath: binPath ? path.join(binPath, 'bin') : undefined,
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
