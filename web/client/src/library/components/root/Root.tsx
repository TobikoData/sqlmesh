
import { FolderTree } from '../folderTree/FolderTree';
import { LogoTobiko } from '../logo/Tobiko'
import { Divider } from '../divider/divider';

import "./Root.css";
import { DropdownPlan, DropdownAudits, DropdownActions } from '../dropdown/dropdown';
import { LogoSqlMesh } from '../logo/SqlMesh';
import { Button } from '../button/button';

export default function Root() {
  return (
    <>
      <Header></Header>
      <Divider />
      <Main>
        <IDE></IDE>
      </Main>
      <Divider />
      <Footer></Footer>
    </>
  );
}


function Header() {
  return (
    <header className='min-h-[3rem] px-2 py-1 flex justify-between items-center'>
      <div className='flex h-full items-center'>
        <LogoTobiko style={{ height: '32px' }} mode='dark'/>
        <Divider className='mx-2 h-[50%]' orientation='vertical' />
        <LogoSqlMesh style={{ height: '32px' }} />
      </div>
      <div>
        <ul className='flex'>
          <li className='px-2 prose underline'>
            Docs
          </li>
          <li className='px-2 prose underline'>
            GitHub
          </li>          
        </ul>
      </div>
    </header>
  )
}

function Main({ children }: { children: React.ReactNode }) {
  return (
    <main className='font-sans w-full h-full flex flex-col overflow-hidden' tabIndex={0}>
      {children}
    </main>
  )
}

function IDE({ project_name = 'Wursthall' }: { project_name?: string }) {
  return (
    <>
      <div className='w-full flex justify-between items-center min-h-[2rem]'>
        
        {/* Project Name */}
        <h3 className='px-4 font-bold'>{project_name}</h3>

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
        {/* <div className='px-4'>users</div> */}
      </div>
      <Divider />
      <div className='flex w-full h-full overflow-hidden'>
        <div className='min-w-[12rem] w-full max-w-[16rem] overflow-hidden overflow-y-auto'>
          <FolderTree />
        </div>
        <Divider orientation='vertical' />
        <div className='h-full w-full flex flex-col overflow-hidden'>

          <div className='w-full overflow-hidden px-2 min-h-[1.5rem] flex items-center'>
            <small className='inline-block cursor-pointer hover:text-gray-300'>audit</small>
            <small className='inline-block px-2'>/</small>
            <small className='inline-block cursor-pointer hover:text-gray-300'>items.sql</small>
          </div>
          
          <Divider />

          <div className='min-w-[20rem] w-full h-full flex overflow-hidden '>
            <div className='w-full h-full flex flex-col'>
              <div className='text-center flex'>
                <div className='p-1 min-w-[10rem] '>
                  Tab 1
                </div>
                <div className='p-1 min-w-[10rem]'>
                  Inactive
                </div>            
                <div className='p-1 w-full'>
                  ide editor tabs
                </div>
              </div>
              <Divider />
              <div className='w-full h-full flex'>
                <div className='h-full w-full p-2'>ide editor</div>
              </div>
              <Divider />
              <div className='px-2 py-1 flex justify-between'>
                <small>
                  ide editor footer
                </small>
                <small>
                  Total char count: 0
                </small>
              </div>
            </div>
            <Divider orientation='vertical' />
            <div className='h-full min-w-[15%] w-full max-w-[25%] p-2'>Inspector</div>
          </div>
          <Divider />
          <div className='w-full min-h-[10rem]'>
            <div className='text-center flex'>
              <div className='p-1 min-w-[10rem]'>
                Table
              </div>
              <div className='p-1 min-w-[10rem]'>
                DAG
              </div>                
              <div className='p-1 w-full'>
                ide preview tabs
              </div>
            </div>
            <Divider />
            <div>ide preview</div>
          </div>
        </div>
      </div>
      <Divider />
      <div className='p-1'>ide footer</div>
    </>
  )
}

function Footer() {
  return (
    <footer className='px-2 py-1 text-xs flex justify-between'>
      <small>        
        {(new Date).getFullYear()}
      </small>
      platform footer
    </footer>
  )
}