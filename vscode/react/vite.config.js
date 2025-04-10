import { defineConfig } from "vite";
import viteReact from "@vitejs/plugin-react";
import { TanStackRouterVite } from "@tanstack/router-plugin/vite";
import { resolve } from "node:path";
import tailwindcss from '@tailwindcss/vite'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [TanStackRouterVite({ autoCodeSplitting: true }), viteReact(), tailwindcss()],
  test: {
    globals: true,
    environment: "jsdom",
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, './src'),
      '@bus': resolve(__dirname, '../bus/src'),
    },
  },
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:5174',
        changeOrigin: true,
        secure: false,
      },
    },
  }
});
