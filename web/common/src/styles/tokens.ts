/**
 * Design Token TypeScript Definitions
 * Type-safe access to CSS custom properties defined in the design system
 */

// Color Tokens
export interface ColorTokens {
  // Brand Colors
  '--color-tobiko': string
  '--color-sqlmesh': string
  '--color-sqlglot': string
  '--color-pacific': string
  '--color-wasabi': string
  '--color-yuzu': string
  '--color-uni': string
  '--color-salmon': string

  // Base Colors
  '--color-white': string
  '--color-black': string
  '--color-cyan': string
  '--color-deep-blue': string
  '--color-purple': string
  '--color-emerald': string
  '--color-mandarin': string
  '--color-scarlet': string
  '--color-sunflower': string
  '--color-peach': string
  '--color-turquoise': string
  '--color-fuchsia': string
  '--color-gray': string

  // Semantic Colors
  '--color-light': string
  '--color-dark': string
  '--color-brand': string
  '--color-prose': string
  '--color-badge-background': string
  '--color-badge-foreground': string
}

// Spacing Tokens
export interface SpacingTokens {
  '--one': string
  '--base': string
  '--half': string
  '--step': string
  '--step-2': string
  '--step-3': string
  '--step-4': string
  '--step-5': string
  '--step-6': string
  '--step-7': string
  '--step-8': string
  '--step-9': string
  '--step-10': string
  '--step-11': string
  '--step-12': string
  '--step-15': string
  '--step-16': string
  '--step-20': string
  '--step-24': string
  '--step-30': string
  '--step-32': string
}

// Typography Tokens
export interface TypographyTokens {
  // Font Families
  '--font-sans': string
  '--font-accent': string
  '--font-serif': string
  '--font-mono': string

  // Font Sizes
  '--font-size': string
  '--text-2xs': string
  '--text-xs': string
  '--text-s': string
  '--text-m': string
  '--text-l': string
  '--text-xl': string
  '--text-2xl': string
  '--text-3xl': string
  '--text-4xl': string
  '--text-headline': string
  '--text-display': string
  '--text-header': string
  '--text-tagline': string
  '--text-title': string
  '--text-subtitle': string

  // Line Heights
  '--leading': string
  '--text-leading-xs': string
  '--text-leading-s': string
  '--text-leading-m': string
  '--text-leading-l': string
  '--text-leading-xl': string

  // Font Weights
  '--font-weight': string
  '--text-thin': string
  '--text-extra-light': string
  '--text-light': string
  '--text-normal': string
  '--text-medium': string
  '--text-semibold': string
  '--text-bold': string
  '--text-extra-bold': string
  '--text-black': string
}

// Combined Design Tokens
export interface DesignTokens
  extends ColorTokens,
    SpacingTokens,
    TypographyTokens {}

// Utility type for accessing CSS custom properties
export type CSSCustomProperty<T extends string> = T

// Type-safe color scale definitions
export type ColorScale =
  | '5'
  | '10'
  | '15'
  | '20'
  | '25'
  | '50'
  | '60'
  | '75'
  | '100'
  | '125'
  | '150'
  | '200'
  | '300'
  | '400'
  | '500'
  | '525'
  | '550'
  | '600'
  | '700'
  | '725'
  | '750'
  | '800'
  | '900'

export type ColorVariant =
  | 'cyan'
  | 'deep-blue'
  | 'pacific'
  | 'purple'
  | 'emerald'
  | 'mandarin'
  | 'scarlet'
  | 'gray'
  | 'uni'
  | 'salmon'
  | 'turquoise'
  | 'fuchsia'

// Helper function to build color custom property strings
export function colorToken(
  variant: ColorVariant,
  scale?: ColorScale,
): CSSCustomProperty<string> {
  return scale ? `--color-${variant}-${scale}` : `--color-${variant}`
}

// Step scale for spacing
export type StepScale =
  | 2
  | 3
  | 4
  | 5
  | 6
  | 7
  | 8
  | 9
  | 10
  | 11
  | 12
  | 15
  | 16
  | 20
  | 24
  | 30
  | 32

// Helper function to build spacing custom property strings
export function spacingToken(
  step?: StepScale | 'half',
): CSSCustomProperty<string> {
  if (step === 'half') return '--half'
  return step ? `--step-${step}` : '--step'
}

// Text size variants
export type TextSize =
  | '2xs'
  | 'xs'
  | 's'
  | 'm'
  | 'l'
  | 'xl'
  | '2xl'
  | '3xl'
  | '4xl'
export type TextRole =
  | 'headline'
  | 'display'
  | 'header'
  | 'tagline'
  | 'title'
  | 'subtitle'

// Helper function to build text size custom property strings
export function textSizeToken(
  size: TextSize | TextRole,
): CSSCustomProperty<string> {
  return `--text-${size}`
}
