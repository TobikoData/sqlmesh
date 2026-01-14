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
        {
          src: 'tailwind.lineage.config.js',
          dest: 'configs',
        },
      ],
    }),
  ],
  resolve: {
    alias: {
      '@sqlmesh-common': path.resolve(__dirname, './src'),
    },
  },
  build: {
    cssMinify: true,
    lib: {
      entry: {
        'sqlmesh-common': path.resolve(__dirname, 'src/index.ts'),
        'lineage/index': path.resolve(
          __dirname,
          'src/components/Lineage/index.ts',
        ),
      },
      name: 'sqlmesh-common',
      fileName: (format, entryName) =>
        ({
          'sqlmesh-common': `sqlmesh-common.${format}.js`,
          'lineage/index': `lineage/index.${format}.js`,
        })[entryName],
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
        '@xyflow/react',
      ],
      output: {
        globals: {
          react: 'React',
          'react-dom': 'ReactDOM',
          clsx: 'clsx',
          'tailwind-merge': 'tailwindMerge',
          'class-variance-authority': 'classVarianceAuthority',
          '@radix-ui/react-slot': 'radixSlot',
          '@xyflow/react': 'xyflowReact',
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
