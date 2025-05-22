import { exec, ExecOptions } from 'node:child_process'
import { traceInfo } from './common/log'

export interface ExecResult {
  exitCode: number
  stdout: string
  stderr: string
}

export async function execAsync(
  command: string,
  args: string[] = [],
  options: ExecOptions & { signal?: AbortSignal } = {},
): Promise<ExecResult> {
  const fullCmd = `${command} ${args.join(' ')}`
  traceInfo(`Executing command: ${fullCmd} in ${options.cwd}`)

  try {
    const result = await execAsyncCore(command, args, options)
    traceInfo(
      `Command ${fullCmd} exited with code ${result.exitCode}; stdout: ${result.stdout}; stderr: ${result.stderr}`,
    )
    return result
  } catch (err) {
    if ((err as any)?.name === 'AbortError') {
      traceInfo(`Command ${fullCmd} was cancelled by AbortController`)
    } else {
      traceInfo(`Command ${fullCmd} failed: ${(err as Error).message}`)
    }
    throw err // keep original error semantics
  }
}

function execAsyncCore(
  command: string,
  args: string[],
  options: ExecOptions & { signal?: AbortSignal } = {},
): Promise<ExecResult> {
  return new Promise<ExecResult>((resolve, reject) => {
    const child = exec(
      `${command} ${args.join(' ')}`,
      options,
      (error, stdout, stderr) => {
        if (error) {
          // Forward AbortError unchanged so callers can detect cancellation
          if ((error as NodeJS.ErrnoException).name === 'AbortError') {
            reject(error)
          } else {
            resolve({
              exitCode: typeof error.code === 'number' ? error.code : 1,
              stdout,
              stderr,
            })
          }
          return
        }

        resolve({
          exitCode: child.exitCode ?? 0,
          stdout,
          stderr,
        })
      },
    )

    // surface “spawn failed” errors that occur before the callback
    child.once('error', reject)
  })
}
