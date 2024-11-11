import path from 'path'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react-swc'

const BASE_URL = process.env.BASE_URL ?? ''
const BASE = BASE_URL ?? '/'

// https://vitejs.dev/config/
export default defineConfig({
  base: BASE,
  resolve: {
    alias: [
      { find: '~', replacement: path.resolve(__dirname, './src') },
      {
        find: '@components',
        replacement: path.resolve(__dirname, './src/library/components'),
      },
      { find: '@hooks', replacement: path.resolve(__dirname, './src/hooks') },
      { find: '@utils', replacement: path.resolve(__dirname, './src/utils') },
      { find: '@models', replacement: path.resolve(__dirname, './src/models') },
      { find: '@api', replacement: path.resolve(__dirname, './src/api') },
      {
        find: '@context',
        replacement: path.resolve(__dirname, './src/context'),
      },
      { find: '@tests', replacement: path.resolve(__dirname, './src/tests') },
    ],
  },
  build: {
    outDir: 'dist',
  },
  define: {
    __BASE_URL__: JSON.stringify(BASE_URL),
    __IS_HEADLESS__: JSON.stringify(Boolean(process.env.IS_HEADLESS ?? false)),
  },
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/tests/setup.ts'],
    exclude: ['**/node_modules/**', './tests'],
  },
  server:
    process.env.NODE_ENV === 'testing'
      ? {}
      : {
          proxy: {
            [`${BASE_URL}/api`]: {
              target: 'http://api:8000',
              rewrite: path => path.replace(`${BASE_URL}/api`, '/api'),
            },
            [`${BASE_URL}/docs`]: {
              target: 'http://app:8001',
              rewrite: path => BASE,
            },
            [`${BASE_URL}/data`]: {
              target: 'http://app:8001',
              rewrite: path => BASE,
            },
            [`${BASE_URL}/lineage`]: {
              target: 'http://app:8001',
              rewrite: path => BASE,
            },
          },
        },
  preview: {
    port: 8005,
  },
})
