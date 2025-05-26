import * as vscode from 'vscode'
import { LSPClient } from '../lsp/lsp'
import { isErr } from '@bus/result'
import { RenderModelEntry } from '../lsp/custom'

export function renderModel(lspClient?: LSPClient) {
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
      const items = result.value.models.map((model: RenderModelEntry) => ({
        label: model.name,
        description: model.fqn,
        detail: model.description,
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

    // Create a new untitled document with the rendered SQL
    const document = await vscode.workspace.openTextDocument({
      language: 'sql',
      content: selectedModel.rendered_query,
    })

    // Determine the view column for side-by-side display
    let viewColumn: vscode.ViewColumn
    if (activeEditor) {
      // Open beside the current editor
      viewColumn = activeEditor.viewColumn
        ? activeEditor.viewColumn + 1
        : vscode.ViewColumn.Two
    } else {
      // If no active editor, open in column two
      viewColumn = vscode.ViewColumn.Two
    }

    // Open the document in the editor as a preview (preview: true is default)
    await vscode.window.showTextDocument(document, {
      viewColumn: viewColumn,
      preview: true,
      preserveFocus: false,
    })
  }
}
