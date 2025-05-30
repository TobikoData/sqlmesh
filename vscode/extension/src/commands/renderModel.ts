import * as vscode from 'vscode'
import { LSPClient } from '../lsp/lsp'
import { isErr } from '@bus/result'
import { RenderModelEntry } from '../lsp/custom'
import { RenderedModelProvider } from '../providers/renderedModelProvider'

const DEBOUNCE_DELAY = 500 // milliseconds - balanced for responsiveness without overwhelming the server

// Track associations between source documents and their rendered views
const sourceToRenderedMap = new Map<string, { uri: vscode.Uri; modelName: string; documentUri: string }>()
// Track file watchers
const fileWatchers = new Map<string, vscode.Disposable>()
// Track active render operations to prevent overlapping requests
const activeRenders = new Map<string, { inProgress: boolean; lastRequestTime: number }>()

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

    // Store the association between source and rendered view
    sourceToRenderedMap.set(documentUri, {
      uri: uri,
      modelName: selectedModel.name,
      documentUri: documentUri
    })

    // Set up a file watcher if not already watching this file
    if (!fileWatchers.has(documentUri)) {
      // Create a debounced update function to avoid too many rapid updates
      let updateTimeout: NodeJS.Timeout | undefined
      let consecutiveErrors = 0
      const debouncedUpdate = () => {
        if (updateTimeout) {
          clearTimeout(updateTimeout)
        }
        
        updateTimeout = setTimeout(() => {
          // Get the stored association
          const association = sourceToRenderedMap.get(documentUri)
          if (!association) return

          // Check if there's already a render in progress
          const renderState = activeRenders.get(documentUri)
          if (renderState?.inProgress) {
            // Skip this update if one is already in progress
            return
          }

          // Mark render as in progress
          activeRenders.set(documentUri, { inProgress: true, lastRequestTime: Date.now() })

          // Re-render the model
          void (async () => {
            try {
              const result = await lspClient.call_custom_method('sqlmesh/render_model', {
                textDocumentUri: documentUri,
              })

              if (isErr(result)) {
                consecutiveErrors++
                console.error(`Failed to re-render model: ${result.error}`)
                
                // Show error message after 3 consecutive failures
                if (consecutiveErrors >= 3) {
                  vscode.window.showWarningMessage(
                    `Model rendering is experiencing issues. Check the SQLMesh output for details.`
                  )
                  consecutiveErrors = 0 // Reset counter after showing message
                }
                return
              }

              // Reset error counter on success
              consecutiveErrors = 0

              // Find the model with the same name
              const model = result.value.models?.find(m => m.name === association.modelName)
              if (model) {
                // Update the content in the provider
                renderedModelProvider.updateRenderedModel(association.uri, model.rendered_query)
              }
            } finally {
              // Mark render as complete
              activeRenders.set(documentUri, { inProgress: false, lastRequestTime: Date.now() })
            }
          })()
        }, DEBOUNCE_DELAY)
      }

      // Watch for changes to the source document
      const watcher = vscode.workspace.onDidChangeTextDocument(event => {
        if (event.document.uri.toString(true) === documentUri) {
          debouncedUpdate()
        }
      })

      fileWatchers.set(documentUri, watcher)
    }

    // Clean up watcher when the rendered document is closed
    const closeListener: vscode.Disposable = vscode.workspace.onDidCloseTextDocument(closedDoc => {
      if (closedDoc.uri.toString() === uri.toString()) {
        const watcher = fileWatchers.get(documentUri)
        if (watcher) {
          watcher.dispose()
          fileWatchers.delete(documentUri)
        }
        sourceToRenderedMap.delete(documentUri)
        activeRenders.delete(documentUri)
        closeListener.dispose()
      }
    })
  }
}

// Export a function to clean up watchers when extension is deactivated
export function disposeRenderModelWatchers(): void {
  fileWatchers.forEach((watcher) => {
    watcher.dispose()
  })
  fileWatchers.clear()
  sourceToRenderedMap.clear()
  activeRenders.clear()
}
