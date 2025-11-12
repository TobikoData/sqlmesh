import { useState } from 'react'

type TimerID = ReturnType<typeof setTimeout>

export function useCopyClipboard(
  delay: number = 2000,
): [(text: string) => void, TimerID | null] {
  const [isCopied, setIsCopied] = useState<TimerID | null>(null)

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    setIsCopied(setTimeout(() => setIsCopied(null), delay))
  }

  return [copyToClipboard, isCopied]
}
