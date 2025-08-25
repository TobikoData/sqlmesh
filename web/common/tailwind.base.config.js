/** @type {import('tailwindcss').Config} */
module.exports = {
  theme: {
    colors: {},
    extend: {
      colors: {
        transparent: 'transparent',
        white: 'var(--color-white)',
        black: 'var(--color-black)',
        dark: 'var(--color-dark)',
        light: 'var(--color-light)',
        brand: 'var(--color-brand)',
        prose: 'var(--color-prose)',
        badge: {
          background: 'var(--color-badge-background)',
          foreground: 'var(--color-badge-foreground)',
        },
      },
      borderRadius: {
        '2xs': 'var(--radius-xs)',
        xs: 'calc(var(--radius-xs) + 1px)',
        sm: 'calc(var(--radius-xs) + 2px)',
        md: 'calc(var(--radius-s))',
        lg: 'calc(var(--radius-s) + 1px)',
        xl: 'calc(var(--radius-s) + 2px)',
        '2xl': 'calc(var(--radius-m))',
      },
      fontSize: {
        '2xs': 'var(--text-2xs)',
        xs: 'var(--text-xs)',
        s: 'var(--text-s)',
        m: 'var(--text-m)',
        l: 'var(--text-l)',
        xl: 'var(--text-xl)',
        '2xl': 'var(--text-2xl)',
        '3xl': 'var(--text-3xl)',
        '4xl': 'var(--text-4xl)',
      },
      fontFamily: {
        mono: ['var(--font-mono)'],
      },
    },
  },
  plugins: [require('@tailwindcss/typography')],
}
