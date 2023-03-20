export const EnumSize = {
  xs: 'xs',
  sm: 'sm',
  md: 'md',
  lg: 'lg',
  xl: 'xl',
} as const

export type Size = (typeof EnumSize)[keyof typeof EnumSize]

export const EnumVariant = {
  Brand: 'brand',
  Primary: 'primary',
  Alternative: 'alternative',
  Secondary: 'secondary',
  Success: 'success',
  Danger: 'danger',
  Warning: 'warning',
  Info: 'info',
  Nutral: 'nutral',
} as const

export type Variant = (typeof EnumVariant)[keyof typeof EnumVariant]
