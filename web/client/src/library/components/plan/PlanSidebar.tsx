import { Context } from '../../../api/client'
import { Divider } from '../divider/Divider'

export function PlanSidebar({ context }: { context?: Context }): JSX.Element {
  return (
    <div className="min-w-[15rem] h-full bg-secondary-100 text-gray-800 overflow-hidden overflow-y-auto">
      <div className="flex flex-col h-full w-full p-4">
        <div className="py-2">
          <h5 className="font-bold text-sm">Engine Adapter</h5>
          <small className="block px-2">{context?.engine_adapter}</small>
        </div>
        <Divider />
        <div className="py-2">
          <h5 className="font-bold text-sm">Config</h5>
          <small className="block px-2">{context?.config}</small>
        </div>
        <Divider />
        <div className="py-2">
          <h5 className="font-bold text-sm">Workers</h5>
          <small className="block px-2">{context?.concurrent_tasks}</small>
        </div>
        <Divider />
        <div className="py-2">
          <h5 className="font-bold text-sm">Scheduler</h5>
          <small className="block px-2">{context?.scheduler}</small>
        </div>
        <Divider />
        <div className="py-2">
          <h5 className="font-bold text-sm">Time Format</h5>
          <small className="block px-2">{context?.time_column_format}</small>
        </div>
        <Divider />
        <div className="flex flex-col py-2 h-full">
          <h5 className="font-bold text-sm">Models</h5>
          {context?.models != null && (
            <ul className="block px-2">
              {context.models.map((tableName: any) => (
                <li
                  key={tableName}
                  className=""
                >
                  <small>{tableName}</small>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  )
}
