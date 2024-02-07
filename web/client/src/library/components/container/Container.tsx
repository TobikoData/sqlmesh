import clsx from 'clsx'

const Container = function Container({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return <>{children}</>
}

function Page({
  children,
  layout = 'vertical',
}: {
  children: React.ReactNode
  layout?: string
}): JSX.Element {
  return (
    <div
      className={clsx(
        'font-sans w-full h-full flex overflow-hidden',
        layout === 'horizontal' ? 'flex-row' : 'flex-col',
      )}
    >
      {children}
    </div>
  )
}

Container.Page = Page

export default Container
