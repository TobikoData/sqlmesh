import { Divider } from '../divider/Divider'
import { IDE } from '../ide/IDE'
import { LogoTobiko } from '../logo/Tobiko'
import { LogoSqlMesh } from '../logo/SqlMesh'

import './Root.css'

export default function Root() {
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

function Header() {
  return (
    <header className="min-h-[3rem] px-2 py-1 flex justify-between items-center">
      <div className="flex h-full items-center">
        <LogoTobiko style={{ height: '32px' }} mode="dark" />
        <Divider className="mx-2 h-[50%]" orientation="vertical" />
        <LogoSqlMesh style={{ height: '32px' }} />
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

function Main({ children }: { children: React.ReactNode }) {
  return (
    <main
      className="font-sans w-full h-full flex flex-col overflow-hidden"
      tabIndex={0}
    >
      {children}
    </main>
  )
}

function Footer() {
  return (
    <footer className="px-2 py-1 text-xs flex justify-between">
      <small>{new Date().getFullYear()}</small>
      platform footer
    </footer>
  )
}
