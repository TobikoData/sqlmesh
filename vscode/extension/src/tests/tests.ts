import * as vscode from 'vscode'
import path from 'path'
import { LSPClient } from '../lsp/lsp'
import { isErr } from '@bus/result'
import { Disposable } from 'vscode'

export const controller = vscode.tests.createTestController(
  'sqlmeshTests',
  'SQLMesh Tests',
)

export const setupTestController = (lsp: LSPClient): Disposable => {
  controller.resolveHandler = async test => {
    console.log('Resolving test:', test?.id)
    const uri = test?.uri
    if (uri) {
      await discoverDocumentTests(uri.toString())
    } else {
      await discoverWorkspaceTests()
    }
  }

  // Discover tests immediately when the controller is set up
  // This is useful for the initial load of tests in the workspace
  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  discoverWorkspaceTests()

  controller.createRunProfile(
    'Run',
    vscode.TestRunProfileKind.Run,
    request => runTests(request),
    true,
  )

  async function discoverDocumentTests(uri: string) {
    const result = await lsp.call_custom_method('sqlmesh/list_document_tests', {
      textDocument: { uri },
    })
    if (isErr(result)) {
      vscode.window.showErrorMessage(
        `Failed to list SQLMesh tests: ${result.error.message}`,
      )
      return
    }
    const fileItem = controller.items.get(uri)
    if (!fileItem) {
      vscode.window.showErrorMessage(`No test item found for document: ${uri}`)
      return
    }
    fileItem.children.replace([])
    for (const test of result.value.tests) {
      const testItem = controller.createTestItem(
        test.name,
        test.name,
        vscode.Uri.parse(test.uri),
      )
      const range = test.range
      testItem.range = new vscode.Range(
        new vscode.Position(range.start.line, range.start.character),
        new vscode.Position(range.end.line, range.end.character),
      )
      fileItem.children.add(testItem)
    }
  }

  async function discoverWorkspaceTests() {
    const result = await lsp.call_custom_method(
      'sqlmesh/list_workspace_tests',
      {},
    )
    if (isErr(result)) {
      vscode.window.showErrorMessage(
        `Failed to list SQLMesh tests: ${result.error.message}`,
      )
      return
    }
    controller.items.replace([])
    const files = new Map<string, vscode.TestItem>()
    for (const entry of result.value.tests) {
      const uri = vscode.Uri.parse(entry.uri)
      let fileItem = files.get(uri.toString())
      if (!fileItem) {
        fileItem = controller.createTestItem(
          uri.toString(),
          path.basename(uri.fsPath),
          uri,
        )
        // THIS IS WHERE YOU RESOLVE THE RANGE
        fileItem.canResolveChildren = true
        files.set(uri.toString(), fileItem)
        controller.items.add(fileItem)
      }
      const testId = `${uri.toString()}::${entry.name}`
      const testItem = controller.createTestItem(testId, entry.name, uri)
      fileItem.children.add(testItem)
    }
  }

  async function runTests(request: vscode.TestRunRequest) {
    const run = controller.createTestRun(request)

    const tests: vscode.TestItem[] = []
    const collect = (item: vscode.TestItem) => {
      if (item.children.size === 0) tests.push(item)
      item.children.forEach(collect)
    }

    if (request.include) request.include.forEach(collect)
    else controller.items.forEach(collect)

    for (const test of tests) {
      run.started(test)
      const startTime = Date.now()
      const uri = test.uri
      if (uri === undefined) {
        run.failed(test, new vscode.TestMessage('Test item has no URI'))
        continue
      }
      const response = await lsp.call_custom_method('sqlmesh/run_test', {
        textDocument: { uri: uri.toString() },
        testName: test.id,
      })
      if (isErr(response)) {
        run.failed(test, new vscode.TestMessage(response.error.message))
        continue
      } else {
        const result = response.value
        const duration = Date.now() - startTime
        if (result.success) {
          run.passed(test, duration)
        } else {
          run.failed(
            test,
            new vscode.TestMessage(result.error_message ?? 'Test failed'),
            duration,
          )
        }
      }
    }
    run.end()
  }

  //  onChangeFile of yaml file reload the tests
  return vscode.workspace.onDidChangeTextDocument(async event => {
    if (event.document.languageId === 'yaml') {
      const uri = event.document.uri.toString()
      const testItem = controller.items.get(uri)
      if (testItem) {
        await discoverDocumentTests(uri)
      } else {
        await discoverWorkspaceTests()
      }
    }
  })
}
