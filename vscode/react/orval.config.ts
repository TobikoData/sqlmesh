import { defineConfig } from 'orval'

export default defineConfig({
  'sqlmesh-api': {
    input: '../openapi.json',
    output: {
      prettier: true,
      target: './src/api/client.ts',
      override: {
        mutator: {
          path: './src/api/instance.ts',
          name: 'fetchAPI',
        },
      },
    },
  },
})
