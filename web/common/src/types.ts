export declare const __brand: unique symbol

export type Brand<B> = { [__brand]: B }
export type Branded<T, B> = T & Brand<B>

export type Size = '2xs' | 'xs' | 's' | 'm' | 'l' | 'xl' | '2xl'
export type HeadlineLevel = 1 | 2 | 3 | 4 | 5 | 6
export type Side = 'left' | 'right' | 'both'
export type LayoutDirection = 'vertical' | 'horizontal' | 'both'
export type Shape = 'square' | 'round' | 'pill'
export type Position =
  | 'top'
  | 'right'
  | 'bottom'
  | 'left'
  | 'center'
  | 'start'
  | 'end'
