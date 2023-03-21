import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useState,
} from 'react'
import useLocalStorage from '~/hooks/useLocalStorage'

export const EnumColorScheme = {
  Light: 'light',
  Dark: 'dark',
} as const

export type ColorScheme = KeyOf<typeof EnumColorScheme>

export interface ThemeMode {
  mode: ColorScheme
  toggleColorScheme?: () => void
}

const initial = {
  mode: EnumColorScheme.Light,
}

export const ThemeContext = createContext<ThemeMode>(initial)

export default function ThemeProvider({
  children,
}: {
  children: ReactNode
}): JSX.Element {
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

  return (
    <ThemeContext.Provider
      value={{
        mode: colorScheme,
        toggleColorScheme() {
          setTheme(
            colorScheme === EnumColorScheme.Light
              ? EnumColorScheme.Dark
              : EnumColorScheme.Light,
          )
        },
      }}
    >
      {children}
    </ThemeContext.Provider>
  )
}

export function useColorScheme(): ThemeMode {
  return useContext(ThemeContext)
}
