import { useState } from 'react'
import { parseJSON } from '~/utils'

const store = window.localStorage

export default function useLocalStorage<T extends Record<string, any>>(
  key: string,
  initialValue?: T,
): [T | undefined, (value: Partial<T>) => void, () => void] {
  const [storedValue, setStoredValue] = useState<T | undefined>(() => {
    try {
      const item = store.getItem(key)

      return item == null ? initialValue : parseJSON(item)
    } catch (error) {
      console.log(error)

      return initialValue
    }
  })

  function setValue(value: Partial<T>): void {
    try {
      const newValue = Object.assign({}, storedValue, value) as T

      store.setItem(key, JSON.stringify(newValue))

      setStoredValue(newValue)
    } catch (error) {
      console.log(error)
    }
  }

  function clear(): void {
    store.removeItem(key)
  }

  return [storedValue, setValue, clear]
}
