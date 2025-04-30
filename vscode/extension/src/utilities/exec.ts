import { exec, ExecOptions } from 'child_process'
import { traceInfo } from './common/log'

export interface ExecResult {
  exitCode: number
  stdout: string
  stderr: string
}

export interface CancellableExecOptions extends ExecOptions {
  /** When `abort()` is called on this signal the child process is killed. */
  signal?: AbortSignal
}

export function execAsync(
  command: string,
  args: string[],
  options: CancellableExecOptions = {},
): Promise<ExecResult> {
  return new Promise((resolve, reject) => {
    // Pass the signal straight through to `exec`
    traceInfo(`Executing command: ${command} ${args.join(' ')}`)
    const child = exec(
      `${command} ${args.join(' ')}`,
      options,
      (error, stdout, stderr) => {
        if (error) {
          resolve({
            exitCode: typeof error.code === 'number' ? error.code : 1,
            stdout,
            stderr,
          })
          return
        }

        resolve({
          exitCode: child.exitCode ?? 0,
          stdout,
          stderr,
        })
      },
    )

    /* ----------  Tie the Promise life‑cycle to the AbortSignal ---------- */

    if (options.signal) {
      // If the caller aborts: kill the child and reject the promise
      const onAbort = () => {
        // `SIGTERM` is the default; use `SIGKILL` if you need something stronger
        child.kill()
        reject(new Error('Process cancelled'))
      }

      if (options.signal.aborted) {
        onAbort()
        return
      }
      options.signal.addEventListener('abort', onAbort, { once: true })

      // Clean‑up the event listener when the promise settles
      const cleanup = () => {
        options.signal!.removeEventListener('abort', onAbort)
      }
      child.once('exit', cleanup)
      child.once('error', cleanup)
    }
  })
}
