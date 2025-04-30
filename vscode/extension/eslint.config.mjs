import typescriptEslint from '@typescript-eslint/eslint-plugin'
import tsParser from '@typescript-eslint/parser'

export default [
  {
    files: ['**/*.ts'],
  },
  {
    plugins: {
      '@typescript-eslint': typescriptEslint,
    },

    languageOptions: {
      parser: tsParser,
      ecmaVersion: 2022,
      sourceType: 'module',
    },

    rules: {
      '@typescript-eslint/naming-convention': [
        'warn',
        {
          selector: 'import',
          format: ['camelCase', 'PascalCase'],
        },
      ],

      curly: 'error',
      eqeqeq: 'error',
      'no-throw-literal': 'error',
      semi: ['error', 'never'],
      'no-shadow': 'error',
    },
  },
]
