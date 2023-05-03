const Container = function Container({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return <>{children}</>
}

function Page({ children }: { children: React.ReactNode }): JSX.Element {
  return (
    <main
      className="font-sans w-full h-full flex flex-col overflow-hidden"
      tabIndex={0}
    >
      {children}
    </main>
  )
}

Container.Page = Page

export default Container
