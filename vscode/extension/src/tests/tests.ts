import * as vscode from 'vscode'
import { LSPClient } from '../lsp/lsp'

export const controller = vscode.tests.createTestController(
  'sqlmeshTests',
  'SQLMesh Tests',
)

export const setupTestController = (lsp: LSPClient) => {
  // First, create the `resolveHandler`. This may initially be called with
  // "undefined" to ask for all tests in the workspace to be discovered, usually
  // when the user opens the Test Explorer for the first time.
  controller.resolveHandler = async test => {
    if (!test) {
      await discoverAllFilesInWorkspace()
    } else {
      await parseTestsInFileContents(test)
    }
  }

  // When text documents are open, parse tests in them.
  vscode.workspace.onDidOpenTextDocument(parseTestsInDocument)
  // We could also listen to document changes to re-parse unsaved changes:
  vscode.workspace.onDidChangeTextDocument(e =>
    parseTestsInDocument(e.document),
  )

  // In this function, we'll get the file TestItem if we've already found it,
  // otherwise we'll create it with `canResolveChildren = true` to indicate it
  // can be passed to the `controller.resolveHandler` to gets its children.
  function getOrCreateFile(uri: vscode.Uri) {
    const existing = controller.items.get(uri.toString())
    if (existing) {
      return existing
    }

    const file = controller.createTestItem(
      uri.toString(),
      uri.path.split('/').pop()!,
      uri,
    )
    file.canResolveChildren = true
    return file
  }

  function parseTestsInDocument(e: vscode.TextDocument) {
    if (e.uri.scheme === 'file' && e.uri.path.endsWith('.md')) {
      parseTestsInFileContents(getOrCreateFile(e.uri), e.getText())
    }
  }

  async function parseTestsInFileContents(
    file: vscode.TestItem,
    contents?: string,
  ) {
    // If a document is open, VS Code already knows its contents. If this is being
    // called from the resolveHandler when a document isn't open, we'll need to
    // read them from disk ourselves.
    if (contents === undefined) {
      const rawContent = await vscode.workspace.fs.readFile(file.uri)
      contents = new TextDecoder().decode(rawContent)
    }

    // some custom logic to fill in test.children from the contents...
  }
}
