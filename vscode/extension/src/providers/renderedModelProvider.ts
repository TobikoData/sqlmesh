import * as vscode from 'vscode'
import { RenderModelEntry } from '../lsp/custom'

interface RenderedModelInfo {
  content: string
  sourceUri?: string
  modelInfo?: RenderModelEntry
}

/**
 * Content provider for read-only rendered SQL models
 */
export class RenderedModelProvider
  implements vscode.TextDocumentContentProvider
{
  private static readonly scheme = 'sqlmesh-rendered'

  // Single map containing all rendered model information
  private renderedModels = new Map<string, RenderedModelInfo>()
  // Track which source file URIs are associated with rendered models
  private sourceToRenderedUri = new Map<string, vscode.Uri>()

  // Event emitter for content changes
  private _onDidChange = new vscode.EventEmitter<vscode.Uri>()
  readonly onDidChange = this._onDidChange.event

  /**
   * Provide text content for a given URI
   */
  provideTextDocumentContent(uri: vscode.Uri): string {
    const key = uri.toString()
    const modelInfo = this.renderedModels.get(key)
    return modelInfo?.content || ''
  }

  /**
   * Store rendered model content and create a URI for it
   */
  storeRenderedModel(
    modelName: string,
    content: string,
    sourceUri?: string,
    modelInfo?: RenderModelEntry,
  ): vscode.Uri {
    const fileName = `${modelName} (rendered)`
    // Add a timestamp to make the URI unique for each render
    const timestamp = Date.now()
    // Use vscode.Uri.from for proper URI construction
    const uri = vscode.Uri.from({
      scheme: RenderedModelProvider.scheme,
      path: fileName,
      fragment: timestamp.toString(),
    })

    const uriString = uri.toString()

    // Store all information in single map
    this.renderedModels.set(uriString, {
      content,
      sourceUri,
      modelInfo,
    })

    // Track the association between a source file and the rendered model
    if (sourceUri) {
      // Remove any existing mapping for this source file
      const existingRenderedUri = this.sourceToRenderedUri.get(sourceUri)
      if (existingRenderedUri) {
        this.renderedModels.delete(existingRenderedUri.toString())
      }

      this.sourceToRenderedUri.set(sourceUri, uri)
    }

    this._onDidChange.fire(uri)
    return uri
  }

  /**
   * Update an existing rendered model with new content
   */
  updateRenderedModel(uri: vscode.Uri, content: string): void {
    const uriString = uri.toString()
    const existingInfo = this.renderedModels.get(uriString)
    if (existingInfo) {
      this.renderedModels.set(uriString, {
        ...existingInfo,
        content,
      })
    }
    this._onDidChange.fire(uri)
  }

  /**
   * Get the rendered URI for a given source file URI
   */
  getRenderedUriForSource(sourceUri: string): vscode.Uri | undefined {
    return this.sourceToRenderedUri.get(sourceUri)
  }

  /**
   * Get the source URI for a given rendered model URI
   */
  getSourceUriForRendered(renderedUri: string): string | undefined {
    const modelInfo = this.renderedModels.get(renderedUri)
    return modelInfo?.sourceUri
  }

  /**
   * Get the model information for a given rendered model URI
   */
  getModelInfoForRendered(
    renderedUri: vscode.Uri,
  ): RenderModelEntry | undefined {
    const modelInfo = this.renderedModels.get(renderedUri.toString())
    return modelInfo?.modelInfo
  }

  /**
   * Check if a source file has an associated rendered model
   */
  hasRenderedModelForSource(sourceUri: string): boolean {
    return this.sourceToRenderedUri.has(sourceUri)
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
    this.sourceToRenderedUri.clear()
    this._onDidChange.dispose()
  }
}
