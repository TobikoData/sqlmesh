import { defineConfig } from 'vite'
import viteReact from '@vitejs/plugin-react'
import { TanStackRouterVite } from '@tanstack/router-plugin/vite'
import { resolve } from 'node:path'
import tailwindcss from '@tailwindcss/vite'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    TanStackRouterVite({ autoCodeSplitting: false }),
    viteReact(),
    tailwindcss(),
  ],
  test: {
    globals: true,
    environment: 'jsdom',
  },

  // This is to ensure we can import the bus module from the bus folder and have nice import paths
  resolve: {
    alias: {
      '@': resolve(__dirname, './src'),
      '@bus': resolve(__dirname, '../bus/src'),
    },
  },

  // This is to ensure that the assets are in the assets folder are all named assets/[name].[extension] rather
  // than having a hash.
  build: {
    // Everything below is pure Rollup syntax
    rollupOptions: {
      output: {
        // ── JavaScript ──────────────────────────────────────
        entryFileNames: 'assets/[name].js', // main-entry
        chunkFileNames: 'assets/[name].js', // code-splits
        // ── CSS & other assets ─────────────────────────────
        assetFileNames: ({ name }) => {
          // name = original file name with extension
          const ext = name?.substring(name.lastIndexOf('.'))
          return `assets/[name]${ext}` // e.g. style.css
        },
      },
    },
  },

  // This ensures that the API calls to the server are proxied to the server.
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:5174',
        changeOrigin: true,
        secure: false,
      },
    },
  },
})
