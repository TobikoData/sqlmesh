import ThemeProvider from '@context/theme'
import { Divider } from '@components/divider/Divider'
import { IDE } from '@components/ide/IDE'
import Header from './Header'
import Footer from './Footer'
import Main from './Main'

export default function Root(): JSX.Element {
  return (
    <ThemeProvider>
      <Header />
      <Divider />
      <Main>
        <IDE />
      </Main>
      <Divider />
      <Footer />
    </ThemeProvider>
  )
}
