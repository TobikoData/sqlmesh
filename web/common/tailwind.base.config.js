import lineageConfig from './tailwind.lineage.config'
import typography from '@tailwindcss/typography'
import scrollbar from 'tailwind-scrollbar'

export default {
  presets: [lineageConfig],
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
        neutral: {
          DEFAULT: 'var(--color-neutral)',
          3: 'var(--color-neutral-3)',
          5: 'var(--color-neutral-5)',
          10: 'var(--color-neutral-10)',
          15: 'var(--color-neutral-15)',
          20: 'var(--color-neutral-20)',
          25: 'var(--color-neutral-25)',
          100: 'var(--color-neutral-100)',
          125: 'var(--color-neutral-125)',
          150: 'var(--color-neutral-150)',
          200: 'var(--color-neutral-200)',
          300: 'var(--color-neutral-300)',
          400: 'var(--color-neutral-400)',
          500: 'var(--color-neutral-500)',
          525: 'var(--color-neutral-525)',
          550: 'var(--color-neutral-550)',
          600: 'var(--color-neutral-600)',
          700: 'var(--color-neutral-700)',
          725: 'var(--color-neutral-725)',
          750: 'var(--color-neutral-750)',
          800: 'var(--color-neutral-800)',
          900: 'var(--color-neutral-900)',
        },
        typography: {
          heading: 'var(--color-typography-headline)',
          tagline: 'var(--color-typography-tagline)',
          description: 'var(--color-typography-description)',
          info: 'var(--color-typography-info)',
        },
        message: {
          translucid: 'var(--color-message-translucid)',
        },
        link: {
          underline: 'var(--color-link-underline)',
          hover: 'var(--color-link-hover)',
          active: 'var(--color-link-active)',
          visited: 'var(--color-link-visited)',
        },
        'model-name': {
          'grayscale-link-underline':
            'var(--color-model-name-grayscale-link-underline)',
          'grayscale-link-underline-hover':
            'var(--color-model-name-grayscale-link-hover)',
          'grayscale-catalog': 'var(--color-model-name-grayscale-catalog)',
          'grayscale-schema': 'var(--color-model-name-grayscale-schema)',
          'grayscale-model': 'var(--color-model-name-grayscale-model)',
          'link-underline': 'var(--color-model-name-link-underline)',
          'link-underline-hover':
            'var(--color-model-name-link-underline-hover)',
          catalog: 'var(--color-model-name-catalog)',
          schema: 'var(--color-model-name-schema)',
          model: 'var(--color-model-name-model)',
          'copy-icon': 'var(--color-model-name-copy-icon)',
          'copy-icon-hover': 'var(--color-model-name-copy-icon-hover)',
          'copy-icon-background':
            'var(--color-model-name-copy-icon-background)',
          'copy-icon-background-hover':
            'var(--color-model-name-copy-icon-background-hover)',
        },
        badge: {
          background: 'var(--color-badge-background)',
          foreground: 'var(--color-badge-foreground)',
        },
        'filterable-list': {
          counter: {
            background: 'var(--color-filterable-list-counter-background)',
            foreground: 'var(--color-filterable-list-counter-foreground)',
          },
          input: {
            background: 'var(--color-filterable-list-input-background)',
            foreground: 'var(--color-filterable-list-input-foreground)',
            placeholder: 'var(--color-filterable-list-input-placeholder)',
            border: 'var(--color-filterable-list-input-border)',
          },
        },
        input: {
          'background-translucid': 'var(--color-input-background-translucid)',
          background: 'var(--color-input-background)',
          foreground: 'var(--color-input-foreground)',
          placeholder: 'var(--color-input-placeholder)',
          border: 'var(--color-input-border)',
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
        tooltip: {
          background: 'var(--color-tooltip-background)',
          foreground: 'var(--color-tooltip-foreground)',
        },
        metadata: {
          label: 'var(--color-metadata-label)',
          value: 'var(--color-metadata-value)',
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
    typography,
    scrollbar({
      nocompatible: true,
      preferredStrategy: 'pseudoelements',
    }),
  ],
}
