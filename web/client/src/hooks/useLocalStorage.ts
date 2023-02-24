import { parseJSON } from '~/utils'

const store = window.localStorage

export default function useLocalStorage<T extends Record<string, any>>(
  key: string,
  initialValue?: T,
): [() => Optional<T>, (value: Partial<T>) => void, () => void] {
  if (initialValue != null) {
    setValue(initialValue)
  }

  function getValue(): Optional<T> {
    try {
      const item = store.getItem(key)

      return parseJSON(item)
    } catch (error) {
      console.log(error)
    }
  }

  function setValue(value: Partial<T>): void {
    try {
      const newValue = Object.assign({}, getValue(), value) as T

      store.setItem(key, JSON.stringify(newValue))
    } catch (error) {
      console.log(error)
    }
  }

  function removeKey(): void {
    store.removeItem(key)
  }

  return [getValue, setValue, removeKey]
}
