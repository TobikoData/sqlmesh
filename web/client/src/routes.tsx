import { createBrowserRouter } from 'react-router-dom'
import IDE from './library/pages/ide/IDE'
import Docs from './library/pages/docs/Docs'
import Editor from './library/pages/editor/Editor'
import IDEProvider from './library/pages/ide/context'

export const EnumRoutes = {
  Ide: '/',
  IdeEditor: '/editor',
  IdeDocs: '/docs',
  IdeDocsModels: '/docs/models',
} as const

export const router = createBrowserRouter([
  {
    path: '/',
    element: (
      <IDEProvider>
        <IDE />
      </IDEProvider>
    ),
    children: [
      {
        path: 'editor',
        element: <Editor />,
      },
      {
        path: 'docs',
        element: <Docs />,
        children: [
          {
            path: 'models/:modelId',
            element: <div>Getting started</div>,
          },
        ],
      },
    ],
  },
])
