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
        focused: 'var(--color-focused)',
        badge: {
          background: 'var(--color-badge-background)',
          foreground: 'var(--color-badge-foreground)',
        },
        button: {
          primary: {
            background: 'var(--color-button-primary-background)',
            foreground: 'var(--color-button-primary-foreground)',
            hover: 'var(--color-button-primary-hover)',
            active: 'var(--color-button-primary-active)',
          },
          secondary: {
            background: 'var(--color-button-secondary-background)',
            foreground: 'var(--color-button-secondary-foreground)',
            hover: 'var(--color-button-secondary-hover)',
            active: 'var(--color-button-secondary-active)',
          },
          alternative: {
            background: 'var(--color-button-alternative-background)',
            foreground: 'var(--color-button-alternative-foreground)',
            hover: 'var(--color-button-alternative-hover)',
            active: 'var(--color-button-alternative-active)',
          },
          destructive: {
            background: 'var(--color-button-destructive-background)',
            foreground: 'var(--color-button-destructive-foreground)',
            hover: 'var(--color-button-destructive-hover)',
            active: 'var(--color-button-destructive-active)',
          },
          danger: {
            background: 'var(--color-button-danger-background)',
            foreground: 'var(--color-button-danger-foreground)',
            hover: 'var(--color-button-danger-hover)',
            active: 'var(--color-button-danger-active)',
          },
          transparent: {
            background: 'var(--color-button-transparent-background)',
            foreground: 'var(--color-button-transparent-foreground)',
            hover: 'var(--color-button-transparent-hover)',
            active: 'var(--color-button-transparent-active)',
          },
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
  plugins: [
    require('@tailwindcss/typography'),
    require('tailwind-scrollbar')({
      nocompatible: true,
      preferredStrategy: 'pseudoelements',
    }),
  ],
}
