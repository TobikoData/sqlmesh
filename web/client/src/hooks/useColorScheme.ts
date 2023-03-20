import { useEffect, useState } from 'react'
import useLocalStorage from './useLocalStorage'

export const EnumColorScheme = {
  Light: 'light',
  Dark: 'dark',
} as const

export type ColorScheme = KeyOf<typeof EnumColorScheme>

export function useColorScheme(): [ColorScheme, () => void] {
  const [getColorSchemeStorage, setColorSchemeStorage] = useLocalStorage<{
    mode: ColorScheme
  }>('color-scheme')
  const colorSchemeLocalStorage =
    getColorSchemeStorage()?.mode ??
    (window.matchMedia('(prefers-color-scheme: dark)').matches
      ? EnumColorScheme.Dark
      : EnumColorScheme.Light)
  const [colorScheme, setTheme] = useState<ColorScheme>(colorSchemeLocalStorage)

  useEffect(() => {
    setColorSchemeStorage({ mode: colorScheme })
    document.documentElement.setAttribute('mode', colorScheme)
  }, [colorScheme])

  return [
    colorScheme,
    () => {
      setTheme(
        colorScheme === EnumColorScheme.Light
          ? EnumColorScheme.Dark
          : EnumColorScheme.Light,
      )
    },
  ]
}
