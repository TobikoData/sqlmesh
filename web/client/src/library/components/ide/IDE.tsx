import { ViewUpdate } from "@codemirror/view";
import Ide from "../../../context/Ide";
import { Button } from "../button/Button";
import { Divider } from "../divider/Divider";
import { DropdownPlan, DropdownAudits } from "../dropdown/Dropdown";
import { Editor } from "../editor/Editor";
import { FolderTree } from "../folderTree/FolderTree";
import Tabs from "../tabs/Tabs";
import { useEffect, useState } from "react";
import { type File } from '../../../api';
import clsx from "clsx";
import { XCircleIcon } from "@heroicons/react/24/solid";

export function IDE({ project_name = 'Wursthall' }: { project_name?: string }) {
  const [files, setFiles] = useState<Set<File>>(new Set())
  const [file, setFile] = useState<File>()

  useEffect(() => {
    console.log({files, file}, files.has(file as File))


    if (files.size === 0) {
      setFile(undefined)
    } else if (!file || !files.has(file)) {
      setFile([...files][0])
    }


    
  }, [files])

  return (
    <Ide.Provider value={{
        files,
        file,
        setFile: e => {
          setFile(e)
          setFiles(new Set([...files, e]))
        }
      }}>
      <div className='w-full flex justify-between items-center min-h-[2rem] z-50'>
        
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
          <Button size="small" variant='alternative'>
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
          </div>
          <Divider /> */}

          {Boolean(file) && (
            <>
              <div className='w-full h-full flex overflow-hidden'>
                <div className='w-full flex flex-col overflow-hidden overflow-x-auto'>
                  <div className='w-full flex min-h-[2rem] overflow-hidden overflow-x-auto'> 
                    <ul className='w-full whitespace-nowrap'>
                      {files.size > 0 && [...files].map((f) => (
                        <li key={f.name} className={clsx(
                          'inline-block justify-between items-center py-1 px-3 overflow-hidden min-w-[10rem] text-center overflow-ellipsis cursor-pointer',
                          f.path === file?.path ? 'bg-white' : 'bg-gray-300'
                        )} onClick={() =>  setFile(f)}>
                          <span className='flex justify-between items-center'>
                            <small>{f.name}</small>
                            <XCircleIcon
                              onClick={e => {
                                e.stopPropagation()
                                files.delete(f)
                                setFiles(new Set(files))
                              }}
                              className={`inline-block text-gray-700 w-4 h-4 ml-2 cursor-pointer`} 
                            /> 
                          </span>
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
                        value={file?.content ?? ""}
                        onChange={(value: string, viewUpdate: ViewUpdate) => {
                          console.log(value, viewUpdate);
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
                        <Button size='small' variant='secondary'>Run Query</Button>
                        <Button size='small' variant='alternative'>Validate</Button>
                        <Button size='small' variant='alternative'>Format</Button>
                        <Button size='small' variant='success'>Save</Button>
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
            </>
          )}

          {!Boolean(file) && (
            <div className='w-full h-full flex justify-center items-center text-center prose'>
              <div>
                <h2>Instractions on how to start</h2>
                <p>
                  Select file
                </p>
              </div>
            </div>
          )}


        </div>
        <Divider orientation='vertical' />
        <div className='min-w-[3.5rem] overflow-hidden py-2'>
          <ul className="flex flex-col items-center">
            <li className="prose text-secondary-500 cursor-pointer text-center w-[2.5rem] h-[2.5rem] rounded-lg bg-secondary-100 flex justify-center items-center mb-2">
              <small>DAG</small>
            </li>
            <li className="prose text-secondary-500 cursor-pointer text-center w-[2.5rem] h-[2.5rem] rounded-lg bg-secondary-100 flex justify-center items-center mb-2">
              <small>QA</small>
            </li>            
          </ul>
        </div>
      </div>
      <Divider />
      <div className='p-1'>ide footer</div>
    </Ide.Provider>
  )
}