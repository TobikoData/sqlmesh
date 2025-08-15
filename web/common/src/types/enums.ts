export const EnumSize = {
  XXS: '2xs',
  XS: 'xs',
  S: 's',
  M: 'm',
  L: 'l',
  XL: 'xl',
  XXL: '2xl',
} as const
export type Size = (typeof EnumSize)[keyof typeof EnumSize]

export const EnumHeadlineLevel = {
  H1: 1,
  H2: 2,
  H3: 3,
  H4: 4,
  H5: 5,
  H6: 6,
} as const
export type HeadlineLevel =
  (typeof EnumHeadlineLevel)[keyof typeof EnumHeadlineLevel]

export const EnumSide = {
  LEFT: 'left',
  RIGHT: 'right',
  BOTH: 'both',
} as const
export type Side = (typeof EnumSide)[keyof typeof EnumSide]

export const EnumLayoutDirection = {
  VERTICAL: 'vertical',
  HORIZONTAL: 'horizontal',
  BOTH: 'both',
} as const
export type LayoutDirection =
  (typeof EnumLayoutDirection)[keyof typeof EnumLayoutDirection]

export const EnumShape = {
  Square: 'square',
  Round: 'round',
  Pill: 'pill',
} as const
export type Shape = (typeof EnumShape)[keyof typeof EnumShape]
