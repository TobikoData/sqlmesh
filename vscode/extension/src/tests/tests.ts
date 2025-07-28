import * as vscode from 'vscode'
import path from 'path'
import { LSPClient } from '../lsp/lsp'
import { isErr } from '@bus/result'
import { sqlmeshExec } from '../utilities/sqlmesh/sqlmesh'
import { execAsync } from '../utilities/exec'

export const controller = vscode.tests.createTestController(
  'sqlmeshTests',
  'SQLMesh Tests',
)

export const setupTestController = (lsp: LSPClient) => {
  controller.resolveHandler = async () => {
    await discoverWorkspaceTests()
  }

  controller.createRunProfile(
    'Run',
    vscode.TestRunProfileKind.Run,
    (request, token) => runTests(request, token),
    true,
  )

  async function discoverWorkspaceTests() {
    const result = await lsp.call_custom_method('sqlmesh/list_workspace_tests', {})
    if (isErr(result)) {
      vscode.window.showErrorMessage(`Failed to list SQLMesh tests: ${result.error.message}`)
      return
    }
    controller.items.replace([])
    const files = new Map<string, vscode.TestItem>()
    for (const entry of result.value.tests) {
      const uri = vscode.Uri.parse(entry.uri)
      let fileItem = files.get(uri.toString())
      if (!fileItem) {
        fileItem = controller.createTestItem(uri.toString(), path.basename(uri.fsPath), uri)
        fileItem.canResolveChildren = true
        files.set(uri.toString(), fileItem)
        controller.items.add(fileItem)
      }
      const testId = `${uri.toString()}::${entry.name}`
      const testItem = controller.createTestItem(testId, entry.name, uri)
      fileItem.children.add(testItem)
    }
  }

  async function runTests(request: vscode.TestRunRequest, token: vscode.CancellationToken) {
    const run = controller.createTestRun(request)
    const exec = await sqlmeshExec()
    if (isErr(exec)) {
      vscode.window.showErrorMessage(`Unable to run tests: ${JSON.stringify(exec.error)}`)
      run.end()
      return
    }

    const tests: vscode.TestItem[] = []
    const collect = (item: vscode.TestItem) => {
      if (item.children.size === 0) tests.push(item)
      item.children.forEach(collect)
    }

    if (request.include) request.include.forEach(collect)
    else controller.items.forEach(collect)

    for (const t of tests) run.started(t)

    const patterns = tests.map(t => {
      const [uriStr, name] = t.id.split('::')
      const uri = vscode.Uri.parse(uriStr)
      return `${uri.fsPath}::${name}`
    })

    const result = await execAsync(exec.value.bin, [...exec.value.args, 'test', ...patterns], {
      cwd: exec.value.workspacePath,
      env: exec.value.env,
      signal: token as unknown as AbortSignal,
    })

    const passed = result.exitCode === 0
    for (const t of tests) {
      if (passed) run.passed(t)
      else run.failed(t, new vscode.TestMessage(result.stderr || 'Failed'))
    }
    run.end()
  }
}
