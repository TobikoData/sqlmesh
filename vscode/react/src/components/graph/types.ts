export const EnumSide = {
  Left: 'left',
  Right: 'right',
} as const

export type Side = (typeof EnumSide)[keyof typeof EnumSide]
