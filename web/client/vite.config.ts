import path from 'path'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react-swc'

// https://vitejs.dev/config/
export default defineConfig({
  resolve: {
    alias: [{ find: '~', replacement: path.resolve(__dirname, './src') }],
  },
  build: {
    outDir: 'prod',
  },
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/tests/setup.ts'],
  },
  server: {
    proxy: {
      '/api': 'http://api:8000',
    },
  },
})
