import { MoonIcon, SunIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import { useColorScheme, EnumColorScheme } from '@context/theme'
import LogoTobiko from '@components/logo/Tobiko'
import LogoSqlMesh from '@components/logo/SqlMesh'

export default function Header(): JSX.Element {
  const { mode, toggleColorScheme } = useColorScheme()

  const IconMoonOrSun = mode === EnumColorScheme.Light ? MoonIcon : SunIcon

  return (
    <header className="min-h-[2rem] px-2 flex justify-between items-center">
      <div className="flex h-full items-center">
        <a
          href="/"
          title="Home"
        >
          <LogoSqlMesh
            style={{ height: '24px' }}
            mode={mode}
          />
        </a>
        <span className="inline-block mx-1 text-xs font-bold">by</span>
        <a
          href="https://tobikodata.com/"
          target="_blank"
          rel="noopener noreferrer"
          title="Tobiko Data website"
        >
          <LogoTobiko
            style={{ height: '20px' }}
            mode={mode}
          />
        </a>
      </div>
      <div className="flex items-center">
        <nav>
          <ul className="flex items-center">
            <li className="px-2">
              <a
                href="http://sqlmesh.readthedocs.io/en/stable/"
                target="_blank"
                rel="noopener noreferrer"
                className="hover:underline text-xs"
              >
                Documentation
              </a>
            </li>
          </ul>
        </nav>
        <button
          className={clsx(
            'p-1 cursor-pointer rounded-full hover:bg-theme-darker',
            'dark:hover:bg-theme-lighter',
          )}
          onClick={() => {
            toggleColorScheme?.()
          }}
          aria-label={
            mode === EnumColorScheme.Light
              ? 'Use Dark Theme'
              : 'Use Light Theme'
          }
        >
          <IconMoonOrSun className="h-4 w-4 text-primary-500" />
        </button>
      </div>
    </header>
  )
}
