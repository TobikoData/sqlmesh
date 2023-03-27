import React from 'react'
import { Divider } from '../divider/Divider'
import { IDE } from '../ide/IDE'
import LogoTobiko from '../logo/Tobiko'
import LogoSqlMesh from '../logo/SqlMesh'
import { SunIcon, MoonIcon } from '@heroicons/react/24/solid'
import clsx from 'clsx'
import ThemeProvider, { EnumColorScheme, useColorScheme } from '~/context/theme'

export default function Root(): JSX.Element {
  return (
    <ThemeProvider>
      <Header></Header>
      <Divider />
      <Main>
        <IDE />
      </Main>
      <Divider />
      <Footer></Footer>
    </ThemeProvider>
  )
}

function Header(): JSX.Element {
  const { mode, toggleColorScheme } = useColorScheme()

  const IconMoonOrSun = mode === EnumColorScheme.Light ? MoonIcon : SunIcon

  return (
    <header className="min-h-[2.5rem] px-2 flex justify-between items-center">
      <div className="flex h-full items-center">
        <LogoSqlMesh
          style={{ height: '32px' }}
          mode={mode}
        />
        <span className="inline-block mx-2 text-xs font-bold">by</span>
        <LogoTobiko
          style={{ height: '24px' }}
          mode={mode}
        />
      </div>
      <div>
        <ul className="flex items-center">
          <li className="px-2">
            <a href="http://sqlmesh.readthedocs.io/en/stable/" target="_blank" rel="noopener noreferrer" className='hover:underline'>
            Documentation
            </a>
          </li>
          <li
            className={clsx(
              'p-2 cursor-pointer rounded-full hover:bg-theme-darker',
              'dark:hover:bg-theme-lighter',
            )}
            onClick={() => {
              toggleColorScheme?.()
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
      <small className='text-xs'>© {new Date().getFullYear()} <a href="http://https://tobikodata.com/" target="_blank" rel="noopener noreferrer" className='underline'>Tobiko Data, Inc.</a> All rights reserved.</small>
    </footer>
  )
}
