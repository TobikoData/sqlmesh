import * as vscode from 'vscode'
import { LSPClient } from '../lsp/lsp'
import { isErr } from '@bus/result'
import { RenderModelEntry } from '../lsp/custom'
import { RenderedModelProvider } from '../providers/renderedModelProvider'

export function renderModel(
  lspClient?: LSPClient,
  renderedModelProvider?: RenderedModelProvider,
) {
  return async () => {
    // Get the current active editor
    const activeEditor = vscode.window.activeTextEditor

    if (!activeEditor) {
      vscode.window.showErrorMessage('No active editor found')
      return
    }

    if (!lspClient) {
      vscode.window.showErrorMessage('LSP client not available')
      return
    }

    // Get the current document URI
    const documentUri = activeEditor.document.uri.toString(true)

    // Call the render model API
    const result = await lspClient.call_custom_method('sqlmesh/render_model', {
      textDocumentUri: documentUri,
    })

    if (isErr(result)) {
      vscode.window.showErrorMessage(`Failed to render model: ${result.error}`)
      return
    }

    // Check if we got any models
    if (!result.value.models || result.value.models.length === 0) {
      vscode.window.showInformationMessage(
        'No models found in the current file',
      )
      return
    }

    // If multiple models, let user choose
    let selectedModel: RenderModelEntry
    if (result.value.models.length > 1) {
      const items = result.value.models.map(model => ({
        label: model.name,
        description: model.fqn,
        detail: model.description ? model.description : undefined,
        model: model,
      }))

      const selected = await vscode.window.showQuickPick(items, {
        placeHolder: 'Select a model to render',
      })

      if (!selected) {
        return
      }

      selectedModel = selected.model
    } else {
      selectedModel = result.value.models[0]
    }

    if (!renderedModelProvider) {
      vscode.window.showErrorMessage('Rendered model provider not available')
      return
    }

    // Store the rendered content and get a virtual URI
    const uri = renderedModelProvider.storeRenderedModel(
      selectedModel.name,
      selectedModel.rendered_query,
    )

    // Open the virtual document
    const document = await vscode.workspace.openTextDocument(uri)

    // Determine the view column for side-by-side display
    // Find the rightmost column with an editor
    let maxColumn = vscode.ViewColumn.One
    for (const editor of vscode.window.visibleTextEditors) {
      if (editor.viewColumn && editor.viewColumn > maxColumn) {
        maxColumn = editor.viewColumn
      }
    }

    // Open in the next column after the rightmost editor
    const viewColumn = maxColumn + 1

    // Open the document in the editor as a preview (preview: true is default)
    await vscode.window.showTextDocument(document, {
      viewColumn: viewColumn,
      preview: true,
      preserveFocus: false,
    })

    // Execute "Keep Open" command to convert preview tab to permanent tab
    await vscode.commands.executeCommand('workbench.action.keepEditor')

    // Explicitly set the language mode to SQL for syntax highlighting
    await vscode.languages.setTextDocumentLanguage(document, 'sql')
  }
}
