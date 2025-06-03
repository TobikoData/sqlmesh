import * as vscode from 'vscode'

/**
 * Content provider for read-only rendered SQL models
 */
export class RenderedModelProvider
  implements vscode.TextDocumentContentProvider
{
  private static readonly scheme = 'sqlmesh-rendered'

  private renderedModels = new Map<string, string>()

  // Event emitter for content changes
  private _onDidChange = new vscode.EventEmitter<vscode.Uri>()
  readonly onDidChange = this._onDidChange.event

  /**
   * Provide text content for a given URI
   */
  provideTextDocumentContent(uri: vscode.Uri): string {
    const key = uri.toString()
    return this.renderedModels.get(key) || ''
  }

  /**
   * Store rendered model content and create a URI for it
   */
  storeRenderedModel(modelName: string, content: string): vscode.Uri {
    const fileName = `${modelName} (rendered)`
    // Add a timestamp to make the URI unique for each render
    const timestamp = Date.now()
    // Use vscode.Uri.from for proper URI construction
    const uri = vscode.Uri.from({
      scheme: RenderedModelProvider.scheme,
      path: fileName,
      fragment: timestamp.toString(),
    })
    this.renderedModels.set(uri.toString(), content)
    this._onDidChange.fire(uri)
    return uri
  }

  /**
   * Get the URI scheme for rendered models
   */
  static getScheme(): string {
    return this.scheme
  }

  /**
   * Clean up old rendered models to prevent memory leaks
   */
  dispose() {
    this.renderedModels.clear()
    this._onDidChange.dispose()
  }
}
