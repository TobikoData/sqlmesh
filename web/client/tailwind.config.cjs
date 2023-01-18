/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
    colors: {
      gray: {
        100: 'hsl(0, 0%, 92%)',
        200: 'hsl(0, 0%, 84%)',
        300: 'hsl(0, 0%, 76%)',
        400: 'hsl(0, 0%, 68%)',
        500: 'hsl(0, 0%, 50%)',
        600: 'hsl(0, 0%, 32%)',
        700: 'hsl(0, 0%, 24%)',
        800: 'hsl(0, 0%, 16%)',
        900: 'hsl(0, 0%, 8%)',
      },
      primary: {
        100: '#fee0cc',
        200: '#ffc199',
        300: '#ffa266',
        500: '#ff8333',
        700: '#cc5100',
      },
      secondary: {
        100: 'hsl(264, 100%, 98%)',
        300: 'hsl(260, 100%, 80%)',
        400: 'hsl(260, 100%, 70%)',
        500: 'hsl(264, 100%, 60%)',
        600: 'hsl(264, 100%, 50%)',
      },
      alternative: {
        500: '#0c81f2',
      },
      success: {
        500: '#0ad96e',
      },
      danger: {
        300: 'hsl(0, 90%, 72%)',
        500: 'hsl(0, 90%, 54%)',
      },
      warning: {
        500: '#f3a322',
      },
      white: '#fff',
      black: '#000',
    },
    fontFamily: {
      sans: ['Circular STD', 'sans-serif'],
      serif: ['Publico', 'serif'],
    },
  },
  plugins: [
    require('@tailwindcss/typography'),
  ],
};
