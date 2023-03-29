import { Divider } from '../divider/Divider'
import { IDE } from '../ide/IDE'

import ThemeProvider from '~/context/theme'
import Header from './Header'
import Footer from './Footer'
import Main from './Main'

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
