const OFF = 0
const ERROR = 2

module.exports = {
  root: true,
  env: {
    browser: true,
    es2021: true,
  },
  extends: ['plugin:react/recommended', 'standard-with-typescript', 'prettier'],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    tsconfigRootDir: __dirname,
    project: './tsconfig.json',
  },
  plugins: ['react', '@typescript-eslint'],
  rules: {
    'react/jsx-uses-react': OFF,
    'react/react-in-jsx-scope': OFF,
    'no-use-before-define': OFF,
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
  },
  ignorePatterns: [
    'src/api/client.ts',
    'test-results',
    'playwright',
    'playwright-report',
    'dist',
  ],
  settings: {
    react: {
      version: '18.2',
    },
  },
}
