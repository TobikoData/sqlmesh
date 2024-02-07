import path from 'path'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react-swc'

// https://vitejs.dev/config/
export default defineConfig({
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
            '/api': {
              target: 'http://api:8000',
            },
            '/docs': {
              target: 'http://app:8001',
              rewrite: path => '/',
            },
            '/data': {
              target: 'http://app:8001',
              rewrite: path => '/',
            },
            '/lineage': {
              target: 'http://app:8001',
              rewrite: path => '/',
            },
          },
        },
  preview: {
    port: 8005,
  },
})
