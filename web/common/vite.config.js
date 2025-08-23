import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'
import dts from 'vite-plugin-dts'
import { viteStaticCopy } from 'vite-plugin-static-copy'

export default defineConfig({
  plugins: [
    react(),
    dts({
      insertTypesEntry: true,
      declarationMap: true,
      tsconfigPath: './tsconfig.build.json',
    }),
    viteStaticCopy({
      targets: [
        {
          src: 'src/styles/design',
          dest: 'styles',
        },
        {
          src: 'tailwind.base.config.js',
          dest: 'configs',
        },
      ],
    }),
  ],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    cssMinify: true,
    lib: {
      entry: path.resolve(__dirname, 'src/index.ts'),
      name: 'sqlmesh-common',
      fileName: format => `sqlmesh-common.${format}.js`,
    },
    rollupOptions: {
      external: [
        'react',
        'react-dom',
        'clsx',
        'tailwind-merge',
        'class-variance-authority',
        '@radix-ui/react-slot',
        'tailwindcss',
        '@tailwindcss/typography',
      ],
      output: {
        globals: {
          react: 'React',
          'react-dom': 'ReactDOM',
          clsx: 'clsx',
          'tailwind-merge': 'tailwindMerge',
          'class-variance-authority': 'classVarianceAuthority',
          '@radix-ui/react-slot': 'radixSlot',
        },
        assetFileNames: assetInfo => {
          if (assetInfo.name && assetInfo.name.endsWith('.css')) {
            return 'styles/[name].min[extname]'
          }
          return '[name][extname]'
        },
      },
    },
    sourcemap: process.env.NODE_ENV !== 'production',
    outDir: 'dist',
  },
})
