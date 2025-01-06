const globals = require('globals')
const eslint = require('@eslint/js')
const prettier = require('eslint-config-prettier')
const tsParser = require('@typescript-eslint/parser')
const tseslint = require('typescript-eslint')
const reactPlugin = require('eslint-plugin-react')

const OFF = 0
const ERROR = 2
const WARN = 1

module.exports = tseslint.config(
  {
    files: ['**/*.{js,jsx,ts,tsx}'],
    languageOptions: {
      parser: tsParser,
      globals: {
        ...globals.serviceworker,
        ...globals.browser,
        ...globals.node,
      },
    },
  },
  {
    ignores: [
      'src/api/client.ts',
      'eslint.config.cjs',
      'test-results',
      'playwright',
      'playwright-report',
      'dist',
    ],
  },
  {
    ...reactPlugin.configs.flat.recommended,
    settings: {
      react: {
        version: '18.3',
      },
    },
  },
  eslint.configs.recommended,
  tseslint.configs.recommended,
  prettier,
  {
    files: ['*.js'],
    languageOptions: {
      parser: undefined, // Use default JS parser for JS files
    },
  },
  {
    rules: {
      'no-unused-expressions': OFF,
      'no-use-before-define': OFF,
      'no-return-await': OFF,
      'react/jsx-uses-react': OFF,
      'react/react-in-jsx-scope': OFF,
      '@typescript-eslint/promise-function-async': OFF,
      '@typescript-eslint/no-non-null-assertion': OFF,
      '@typescript-eslint/return-await': OFF,
      '@typescript-eslint/no-use-before-define': [
        ERROR,
        {
          variables: true,
          functions: false,
          classes: false,
          allowNamedExports: true,
        },
      ],
      '@typescript-eslint/no-dynamic-delete': OFF,
      '@typescript-eslint/naming-convention': [
        ERROR,
        {
          selector: 'variable',
          format: ['camelCase', 'PascalCase', 'UPPER_CASE', 'snake_case'],
        },
      ],
      '@typescript-eslint/no-confusing-void-expression': OFF,
      '@typescript-eslint/no-empty-object-type': OFF,
      '@typescript-eslint/no-unused-expressions': OFF,
      '@typescript-eslint/no-explicit-any': WARN,
    },
  },
)
