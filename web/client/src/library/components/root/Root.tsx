import React from 'react'
import { Divider } from '../divider/Divider'
import { IDE } from '../ide/IDE'
import LogoTobiko from '../logo/Tobiko'
import LogoSqlMesh from '../logo/SqlMesh'

import './Root.css'

export default function Root(): JSX.Element {
  return (
    <>
      <Header></Header>
      <Divider />
      <Main>
        <IDE></IDE>
      </Main>
      <Divider />
      <Footer></Footer>
    </>
  )
}

function Header(): JSX.Element {
  return (
    <header className="min-h-[2.5rem] px-2 flex justify-between items-center">
      <div className="flex h-full items-center">
        <LogoSqlMesh style={{ height: '32px' }} />
        <span className="inline-block mx-2 text-xs font-bold">by</span>
        <LogoTobiko
          style={{ height: '24px' }}
          mode="dark"
        />
      </div>
      <div>
        <ul className="flex">
          <li className="px-2 prose underline">Docs</li>
          <li className="px-2 prose underline">GitHub</li>
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
