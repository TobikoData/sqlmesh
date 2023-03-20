type Size = (typeof EnumSize)[keyof typeof EnumSize]
type Variant = (typeof EnumVariant)[keyof typeof EnumVariant]
type Subset<T, S extends T> = S
type Path = string
type ID = string | number
type KeyOf<T> = T[keyof T]
type Optional<T> = T | undefined
