import React from 'react'
import { Divider } from '../divider/Divider'
import { IDE } from '../ide/IDE'
import LogoTobiko from '../logo/Tobiko'
import LogoSqlMesh from '../logo/SqlMesh'
import { SunIcon, MoonIcon } from '@heroicons/react/24/solid'
import { EnumColorScheme, useColorScheme } from '~/hooks/useColorScheme'
import clsx from 'clsx'

export default function Root(): JSX.Element {
  return (
    <>
      <Header></Header>
      <Divider />
      <Main>
        <IDE />
      </Main>
      <Divider />
      <Footer></Footer>
    </>
  )
}

function Header(): JSX.Element {
  const [colorScheme, toggleColorScheme] = useColorScheme()

  const IconMoonOrSun =
    colorScheme === EnumColorScheme.Light ? MoonIcon : SunIcon

  return (
    <header className="min-h-[2.5rem] px-2 flex justify-between items-center">
      <div className="flex h-full items-center">
        <LogoSqlMesh
          style={{ height: '32px' }}
          mode={colorScheme}
        />
        <span className="inline-block mx-2 text-xs font-bold">by</span>
        <LogoTobiko
          style={{ height: '24px' }}
          mode={colorScheme}
        />
      </div>
      <div>
        <ul className="flex items-center">
          <li className="px-2">Docs</li>
          <li className="px-2">GitHub</li>
          <li
            className={clsx(
              'p-2 cursor-pointer rounded-full hover:bg-theme-darker',
              'dark:hover:bg-theme-lighter',
            )}
            onClick={() => {
              toggleColorScheme()
            }}
          >
            <IconMoonOrSun className="h-4 w-4 text-primary-500" />
          </li>
        </ul>
      </div>
    </header>
  )
}

function Main({ children }: { children: React.ReactNode }): JSX.Element {
  return (
    <main
      className="font-sans w-full h-full flex flex-col overflow-hidden"
      tabIndex={0}
    >
      {children}
    </main>
  )
}

function Footer(): JSX.Element {
  return (
    <footer className="px-2 py-1 text-xs flex justify-between">
      <small>{new Date().getFullYear()}</small>
      platform footer
    </footer>
  )
}
