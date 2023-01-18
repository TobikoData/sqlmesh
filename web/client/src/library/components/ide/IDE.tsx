import { ViewUpdate } from "@codemirror/view";
import Ide from "../../../context/Ide";
import { Button } from "../button/Button";
import { Divider } from "../divider/Divider";
import { DropdownPlan, DropdownAudits } from "../dropdown/Dropdown";
import { Editor } from "../editor/Editor";
import { FolderTree } from "../folderTree/FolderTree";
import Tabs from "../tabs/Tabs";
import { useState } from "react";
import { type File } from '../../../api';
import clsx from "clsx";

export function IDE({ project_name = 'Wursthall' }: { project_name?: string }) {
  const [files, setFiles] = useState<Set<any>>(new Set())
  const [file, setFile] = useState<File>()

  return (
    <Ide.Provider value={{
        files,
        file,
        setFile: e => {
          setFile(e)
          setFiles(new Set([...files, e]))
        }
      }}>
      <div className='w-full flex justify-between items-center min-h-[2rem]'>
        
        {/* Project Name */}
        <h3 className='px-3 font-bold'>{project_name}</h3>

        {/* Git */}
        {/* <div className='px-4 flex'>
          <div className='px-4'>TobikoData/wursthall</div>
          <div className='px-4'>main</div>
        </div> */}

        {/* Search */}
        {/* <div className='px-4 w-full'>search</div> */}

        <div className='px-4 flex items-center'>
          <DropdownPlan />
          <DropdownAudits />
          <Button className='bg-gray-500 text-gray-100'>
            Run Tests
          </Button>        
          
        </div>
      </div>
      <Divider />
      <div className='flex w-full h-full overflow-hidden'>
        <div className='min-w-[12rem] w-full max-w-[16rem] overflow-hidden overflow-y-auto'>
          <FolderTree />
        </div>
        <Divider orientation='vertical' />
        <div className='h-full w-full flex flex-col overflow-hidden'>

          {/* Breadcrubms */}
          {/* <div className='w-full overflow-hidden px-2 min-h-[1.5rem] flex items-center'>
            <small className='inline-block cursor-pointer hover:text-gray-300'>audit</small>
            <small className='inline-block px-2'>/</small>
            <small className='inline-block cursor-pointer hover:text-gray-300'>items.sql</small>
          </div> */}
          
          {/* <Divider /> */}

          <div className='w-full h-full flex overflow-hidden'>
            <div className='w-full flex flex-col overflow-hidden overflow-x-auto'>
              <div className='w-full flex min-h-[2rem] overflow-hidden overflow-x-auto'> 
                <ul className='w-full whitespace-nowrap flex'>
                  {files.size > 0 && [...files].map((f) => (
                    <li key={f.name} className={clsx(
                      'inline-block py-1 px-3 overflow-hidden min-w-[10rem] text-center overflow-ellipsis hover:bg-gray-200 cursor-pointer',
                      f.id === file?.id ? 'bg-gray-500' : 'bg-gray-100'
                    )} onClick={() =>  setFile(f)}>
                      <small>{f.name}</small>
                      <span onClick={() => {
                        files.delete(file)
                        setFiles(new Set([...files]))
                      }}>x</span>
                    </li>
                  ))}
                </ul>
              </div>

              {/* <div className='text-center flex'>
                <div className='p-1 min-w-[10rem] '>
                  Tab 1
                </div>
                <div className='p-1 min-w-[10rem]'>
                  Inactive
                </div>            
                <div className='p-1 w-full'>
                  ide editor tabs
                </div>
              </div> */}
              {/* <Divider /> */}
              <div className='w-full h-full flex flex-col overflow-hidden'>
                <div className='w-full h-full overflow-hidden'>
                  <Editor
                    className='h-full w-full'
                    extension={file?.extension}
                    value={file?.value}
                    onChange={(value: string, viewUpdate: ViewUpdate) => {
                      // console.log(value, viewUpdate);
                    }}
                  />
                </div>

              </div>
              <Divider />
              <div className='px-2 py-1 flex justify-between min-h-[1.5rem]'>
                <small>
                  validation: ok
                </small>
                <div className='flex'>
                    <Button className='bg-secondary-500 text-gray-100'>Run Query</Button>
                    <Button className='bg-gray-500 text-gray-100'>Validate</Button>
                    <Button className='bg-gray-500 text-gray-100'>Format</Button>
                    <Button className='bg-success-500 text-gray-100'>Save</Button>
                  </div>
              </div>           
            </div>
                                  
            {/* <Divider orientation='vertical' /> */}
            {/* <div className='h-full min-w-[15%] w-full max-w-[25%] p-2'>Inspector</div> */}
          </div>
          <Divider />
          <div className='w-full min-h-[10rem] overflow-auto'>
            {/* <div className='text-center flex'>
              <div className='p-1 min-w-[10rem]'>
                Table
              </div>
              <div className='p-1 min-w-[10rem]'>
                DAG
              </div>                
              <div className='p-1 w-full'>
                ide preview tabs
              </div>
            </div> */}
            {/* <Divider /> */}
            <Tabs />
          </div>
        </div>
      </div>
      <Divider />
      <div className='p-1'>ide footer</div>
    </Ide.Provider>
  )
}