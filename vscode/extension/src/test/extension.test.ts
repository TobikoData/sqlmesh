import * as assert from 'assert'

// You can import and use all API from the 'vscode' module
// as well as import your extension to test it
import * as vscode from 'vscode'
// import * as myExtension from '../../extension';

suite('Extension Test Suite', () => {
  vscode.window.showInformationMessage('Start all tests.')

  test('Sample test', () => {
    assert.strictEqual(-1, [1, 2, 3].indexOf(5))
    assert.strictEqual(-1, [1, 2, 3].indexOf(0))
  })

  test('Lineage tab opens', async () => {
    // Check if command exists
    const allCommands = await vscode.commands.getCommands(true)
    const hasLineageViewCommand = allCommands.includes(
      'workbench.view.extension.lineage_view',
    )

    if (!hasLineageViewCommand) {
      throw new Error('Lineage view command not found')
    }
    // Try to open the Lineage webview
    await vscode.commands.executeCommand(
      'workbench.view.extension.lineage_view',
    )

    // If command exists but fails, the test will fail with an exception
    // Wait a bit for any async operations
    await new Promise(resolve => setTimeout(resolve, 500))

    // Success if no exception was thrown
    assert.ok(true, 'Lineage view command executed without errors')
  })
})
