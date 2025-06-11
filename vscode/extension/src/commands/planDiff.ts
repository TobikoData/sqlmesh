import * as vscode from 'vscode'
import { LSPClient } from '../lsp/lsp'
import { isErr } from '@bus/result'

export function planDiff(lspClient?: LSPClient) {
  return async () => {
    if (!lspClient) {
      vscode.window.showErrorMessage('LSP client not available')
      return
    }

    const envResult = await lspClient.call_custom_method(
      'sqlmesh/list_environments',
      {},
    )

    if (isErr(envResult)) {
      vscode.window.showErrorMessage(
        `Failed to list environments: ${envResult.error.message}`,
      )
      return
    }

    const env = await vscode.window.showQuickPick(envResult.value.environments, {
      placeHolder: 'Select environment to plan',
    })

    if (!env) {
      return
    }

    const diffResult = await lspClient.call_custom_method('sqlmesh/plan_diff', {
      environment: env,
    })

    if (isErr(diffResult)) {
      vscode.window.showErrorMessage(
        `Failed to get plan diff: ${diffResult.error.message}`,
      )
      return
    }

    if (!diffResult.value.diffs.length) {
      vscode.window.showInformationMessage('No changes detected')
      return
    }

    let selected = diffResult.value.diffs[0]
    if (diffResult.value.diffs.length > 1) {
      const pick = await vscode.window.showQuickPick(
        diffResult.value.diffs.map(d => ({ label: d.name })),
        { placeHolder: 'Select a model diff to view' },
      )
      if (!pick) {
        return
      }
      selected = diffResult.value.diffs.find(d => d.name === pick.label)!
    }

    const doc = await vscode.workspace.openTextDocument({
      content: selected.diff,
      language: 'diff',
    })
    await vscode.window.showTextDocument(doc, { preview: true })
  }
}
