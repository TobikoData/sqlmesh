import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import path from 'path'
// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    proxy: process.env.NODE_ENV === 'development' ? {
      // Proxy API requests to the SQLMesh server during development
      '/api': {
        target: 'http://localhost:5174',
        changeOrigin: true,
        secure: false,
      }
    } : undefined,
  },
})
