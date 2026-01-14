import baseConfig from './tailwind.base.config'

export default {
  presets: [baseConfig],
  content: ['./src/**/*.{js,ts,jsx,tsx}', './src/**/*.stories.{js,ts,jsx,tsx}'],
}
