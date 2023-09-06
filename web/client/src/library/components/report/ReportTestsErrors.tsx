import { type TestReportError } from '@components/plan/context'

export default function ReportTestsErrors({
  report,
}: {
  report: TestReportError
}): JSX.Element {
  return (
    <div>
      <div className="py-2">
        <p>Total: {report.total}</p>
        <p>Succeeded: {report.successful}</p>
        <p>Failed: {report.failures}</p>
        <p>Errors: {report.errors}</p>
        <p>Dialect: {report.dialect}</p>
      </div>
      <ul>
        {report.details?.map(item => (
          <li
            key={item.message}
            className="flex mb-1"
          >
            <span className="inline-block mr-4">&mdash;</span>
            <div className="overflow-hidden">
              <span className="inline-block mb-2">{item.message}</span>
              <code className="inline-block max-h-[50vh] bg-theme py-2 px-4 rounded-lg w-full overflow-auto hover:scrollbar scrollbar--vertical scrollbar--horizontal">
                <pre>{item.details}</pre>
              </code>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}
