export default function Main({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return (
    <main
      className="font-sans w-full h-full flex flex-col overflow-hidden"
      tabIndex={0}
    >
      {children}
    </main>
  )
}
