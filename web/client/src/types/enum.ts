
export const EnumSize = {
  xs: 'xs',
  sm: 'sm',
  md: 'md',
  lg: 'lg',
  xl: 'xl',
} as const;

declare global {
  type Size = typeof EnumSize[keyof typeof EnumSize]
  type Subset<T, S extends T> = S
}