import { cn } from '@/utils'
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import { vs } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { CopyButton } from '../CopyButton/CopyButton'
import { VerticalContainer } from '../VerticalContainer/VerticalContainer'

export function Code({
  content,
  language = 'sql',
  className,
  hideNumbers = false,
  wrap = false,
  showCopy = true,
}: {
  content: string
  language?: 'sql' | 'python' | 'yaml' | 'json' | 'diff'
  hideNumbers?: boolean
  wrap?: boolean
  showCopy?: boolean
  className?: string
}) {
  return (
    <VerticalContainer
      data-component="Code"
      className={cn(
        'relative p-2 rounded-2xl border border-neutral-10 font-mono text-xs',
        className,
      )}
    >
      {showCopy && (
        <div className="flex justify-end flex-shrink-0 absolute top-2 right-4">
          <CopyButton text={content}>
            {copied => (copied ? 'Copied!' : 'Copy')}
          </CopyButton>
        </div>
      )}
      <VerticalContainer scroll>
        <SyntaxHighlighter
          language={language}
          style={vs}
          showLineNumbers={!hideNumbers}
          wrapLines={wrap}
        >
          {content}
        </SyntaxHighlighter>
      </VerticalContainer>
    </VerticalContainer>
  )
}
