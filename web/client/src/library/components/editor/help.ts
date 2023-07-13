import { type EditorTab, type Dialect } from '@context/editor'
import { EnumFileExtensions, type FileExtensions } from '@models/file'
import { isArrayNotEmpty } from '~/utils'

export function getLanguageByExtension(extension?: FileExtensions): string {
  switch (extension) {
    case EnumFileExtensions.SQL:
      return 'SQL'
    case EnumFileExtensions.PY:
      return 'Python'
    case EnumFileExtensions.YAML:
    case EnumFileExtensions.YML:
      return 'YAML'
    default:
      return 'Plain Text'
  }
}

export function showIndicatorDialects(
  tab: EditorTab,
  dialects: Dialect[],
): boolean {
  return (
    tab.file.extension === EnumFileExtensions.SQL &&
    tab.file.isLocal &&
    isArrayNotEmpty(dialects)
  )
}
