import eslint from '@eslint/js'
import tseslint from 'typescript-eslint'

export default tseslint.config(
  {
    ignores: [
      'dist/**/*',
      'node_modules/**',
      'src/utils/tbk-components.js',
      '**/*.cjs',
      '**/*.mjs',
      'src/api/client.ts',
    ],
  },
  eslint.configs.recommended,
  tseslint.configs.recommended,
  {
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unused-vars': 'off',
      '@typescript-eslint/no-empty-object-type': 'off',
      '@typescript-eslint/no-unused-expressions': 'off',
      'no-empty': 'off',
    },
  },
)
