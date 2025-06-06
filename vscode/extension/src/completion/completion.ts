import * as vscode from 'vscode'
import { LSPClient } from '../lsp/lsp'
import { isErr } from '@bus/result'

export const selector: vscode.DocumentSelector = {
  pattern: '**/*.sql',
}

export const completionProvider = (
  lsp: LSPClient,
): vscode.CompletionItemProvider => {
  return {
    async provideCompletionItems(document) {
      const result = await lsp.call_custom_method('sqlmesh/all_models', {
        textDocument: {
          uri: document.uri.fsPath,
        },
      })
      if (isErr(result)) {
        return []
      }
      const modelCompletions = result.value.models.map(
        model =>
          new vscode.CompletionItem(model, vscode.CompletionItemKind.Reference),
      )
      const macroCompletions = result.value.macros.map(
        macro =>
          new vscode.CompletionItem(macro, vscode.CompletionItemKind.Function),
      )
      const keywordCompletions = result.value.keywords.map(
        keyword =>
          new vscode.CompletionItem(keyword, vscode.CompletionItemKind.Keyword),
      )
      return new vscode.CompletionList([
        ...modelCompletions,
        ...macroCompletions,
        ...keywordCompletions,
      ])
    },
  }
}
