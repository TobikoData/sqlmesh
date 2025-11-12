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
  bgTestSuccess: 'bg-[var(--vscode-testing-iconPassed)]',
  bgError: 'bg-[var(--vscode-testing-iconFailed)]',
  bgWarning: 'bg-[var(--vscode-testing-iconQueued)]',
  bgInfo: 'bg-[var(--vscode-testing-iconUnset)]',

  // Border colors
  borderPanel: 'border-[var(--vscode-panel-border)]',
  borderInfo: 'border-[var(--vscode-testing-iconUnset)]',
  borderSuccess: 'border-[var(--vscode-testing-iconPassed)]',
  borderError: 'border-[var(--vscode-diffEditor-removedTextForeground)]',
  borderWarning: 'border-[var(--vscode-diffEditor-modifiedTextForeground)]',
  borderAdded: 'border-[var(--vscode-diffEditor-insertedTextForeground)]',
  borderRemoved: 'border-[var(--vscode-diffEditor-removedTextForeground)]',
  borderModified: 'border-[var(--vscode-diffEditor-modifiedTextForeground)]',

  //These colors are similar to web UI
  // Primary (blue)
  textPrimary: 'text-[#3b82f6]',
  bgPrimary10: 'bg-[#3b82f6]/10',
  bgPrimary: 'bg-[#3b82f6]',
  borderPrimary: 'border-[#3b82f6]',

  // Success (green)
  textSuccess500: 'text-[#10b981]',
  bgSuccess10: 'bg-[#10b981]/10',
  bgSuccess: 'bg-[#10b981]',
  borderSuccess500: 'border-[#10b981]',

  // Danger (red)
  textDanger500: 'text-[#ef4444]',
  bgDanger5: 'bg-[#ef4444]/5',
  bgDanger10: 'bg-[#ef4444]/10',
  bgDanger: 'bg-[#ef4444]',
  borderDanger500: 'border-[#ef4444]',

  // Brand (purple)
  textBrand: 'text-[#8b5cf6]',
  bgBrand10: 'bg-[#8b5cf6]/10',
  bgBrand: 'bg-[#8b5cf6]',
  borderBrand500: 'border-[#8b5cf6]',

  // Neutral
  bgNeutral5: 'bg-[var(--vscode-editor-inactiveSelectionBackground)]',
  bgNeutral10: 'bg-[var(--vscode-list-hoverBackground)]',
  textNeutral500: 'text-[var(--vscode-descriptionForeground)]',
  textNeutral600: 'text-[var(--vscode-editor-foreground)]',
  borderNeutral100: 'border-[var(--vscode-panel-border)]',
}

// Helper function to combine conditional classes
export function twMerge(...classes: (string | false | undefined | null)[]) {
  return classes.filter(Boolean).join(' ')
}
