// Tailwind utility classes with CSS variables
export const twColors = {
  // Text colors
  textForeground: 'text-[var(--vscode-editor-foreground)]',
  textInfo: 'text-[var(--vscode-testing-iconUnset)]',
  textSuccess: 'text-[var(--vscode-testing-iconPassed)]',
  textError: 'text-[var(--vscode-testing-iconFailed)]',
  textWarning: 'text-[var(--vscode-testing-iconQueued)]',
  textMuted: 'text-[var(--vscode-descriptionForeground)]',
  textAccent: 'text-[var(--vscode-textLink-foreground)]',
  textAdded: 'text-[var(--vscode-diffEditor-insertedTextForeground)]',
  textRemoved: 'text-[var(--vscode-diffEditor-removedTextForeground)]',
  textModified: 'text-[var(--vscode-diffEditor-modifiedTextForeground)]',

  // Source and target environment colors
  textSource: 'text-[var(--vscode-debugIcon-continueForeground)]',
  textTarget: 'text-[var(--vscode-debugIcon-startForeground)]',
  textClass: 'text-[var(--vscode-symbolIcon-classForeground)]',
  bgSource: 'bg-[var(--vscode-debugIcon-continueForeground)]',
  bgTarget: 'bg-[var(--vscode-debugIcon-startForeground)]',
  bgClass: 'bg-[var(--vscode-symbolIcon-classForeground)]',
  borderSource: 'border-[var(--vscode-debugIcon-continueForeground)]',
  borderTarget: 'border-[var(--vscode-debugIcon-startForeground)]',
  borderClass: 'border-[var(--vscode-symbolIcon-classForeground)]',

  // Background colors
  bgEditor: 'bg-[var(--vscode-editor-background)]',
  bgInput: 'bg-[var(--vscode-input-background)]',
  bgHover: 'hover:bg-[var(--vscode-list-hoverBackground)]',
  bgInactiveSelection: 'bg-[var(--vscode-editor-inactiveSelectionBackground)]',
  bgAdded: 'bg-[var(--vscode-diffEditor-insertedTextBackground)]',
  bgRemoved: 'bg-[var(--vscode-diffEditor-removedTextBackground)]',
  bgModified: 'bg-[var(--vscode-diffEditor-modifiedTextBackground)]',

  // Border colors
  borderPanel: 'border-[var(--vscode-panel-border)]',
  borderInfo: 'border-[var(--vscode-testing-iconUnset)]',
  borderSuccess: 'border-[var(--vscode-testing-iconPassed)]',
  borderError: 'border-[var(--vscode-diffEditor-removedTextForeground)]',
  borderWarning: 'border-[var(--vscode-diffEditor-modifiedTextForeground)]',
  borderAdded: 'border-[var(--vscode-diffEditor-insertedTextForeground)]',
  borderRemoved: 'border-[var(--vscode-diffEditor-removedTextForeground)]',
  borderModified: 'border-[var(--vscode-diffEditor-modifiedTextForeground)]',
}

// Helper function to combine conditional classes
export function twMerge(...classes: (string | false | undefined | null)[]) {
  return classes.filter(Boolean).join(' ')
}
