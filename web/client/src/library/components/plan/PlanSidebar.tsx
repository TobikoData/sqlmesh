import { Divider } from "../divider/Divider";

export function PlanSidebar({ context }: any) {
  return (
    <div className="min-w-[15rem] h-full bg-secondary-900 text-gray-100 overflow-hidden overflow-y-auto">
      <div className="flex flex-col h-full w-full p-4">
        <div className="py-2">
          <h5 className="font-bold text-sm">Engine Adapter</h5>
          <small className="block px-2">{context?.engine_adapter}</small>
        </div>
        <Divider />
        <div className="py-2">
          <h5 className="font-bold text-sm">Path</h5>
          <small className="block px-2">{context?.path}</small>
        </div>
        <Divider />
        <div className="py-2">
          <h5 className="font-bold text-sm">Dialect</h5>
          <small className="block px-2">{context?.dialect}</small>
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
          <ul className="block px-2">
            {context?.models.map((tableName: any) => (
              <li key={tableName} className="">
                <small>{tableName}</small>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  )
}