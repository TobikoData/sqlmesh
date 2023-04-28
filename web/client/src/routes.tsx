import { createBrowserRouter } from 'react-router-dom'
import IDE from './library/pages/ide/IDE'
import Docs from './library/pages/docs/Docs'

export const router = createBrowserRouter([
  {
    path: '/',
    element: <IDE />,
  },
  {
    path: '/docs',
    element: <Docs />,
  },
])
