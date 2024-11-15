import React, { type HTMLAttributes } from 'react'
import ThemeProvider from '@context/theme'
import ReactDOM from 'react-dom/client'
import { type ApiQueryMeta } from './api'
import {
  QueryCache,
  QueryClient,
  QueryClientProvider,
} from '@tanstack/react-query'
import App from './App'
import NotificationCenterProvider, {
  type ErrorIDE,
} from './library/pages/root/context/notificationCenter'
import { isNotNil } from './utils'

import './styles/index.css'

export interface PropsComponent extends HTMLAttributes<HTMLElement> {}

const client = new QueryClient({
  queryCache: new QueryCache({
    onError(error, query) {
      ;(query.meta as ApiQueryMeta).onError(error as ErrorIDE)
    },
    onSuccess(_, query) {
      ;(query.meta as ApiQueryMeta).onSuccess()
    },
  }),
  defaultOptions: {
    queries: {
      networkMode: 'always',
      refetchOnWindowFocus: false,
      retry: false,
      staleTime: Infinity,
    },
  },
})

ReactDOM.createRoot(getRootNode()).render(
  <React.StrictMode>
    <QueryClientProvider client={client}>
      <ThemeProvider>
        <NotificationCenterProvider>
          <App />
        </NotificationCenterProvider>
      </ThemeProvider>
    </QueryClientProvider>
  </React.StrictMode>,
)

function getRootNode(): HTMLElement {
  const id = 'root'

  let elRoot = document.getElementById(id)

  if (isNotNil(elRoot)) return elRoot

  const elBody = document.body
  const firstChild = elBody.children[0]

  elRoot = document.createElement('div')
  elRoot.id = id
  elRoot.className = 'h-full w-full flex flex-col justify-start'

  if (firstChild instanceof HTMLElement) {
    elBody.insertBefore(elRoot, firstChild)
  } else {
    elBody.appendChild(elRoot)
  }

  return elRoot
}
