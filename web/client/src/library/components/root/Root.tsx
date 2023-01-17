import FolderTree from '../folderTree/FolderTree';
import LogoTobiko from '../logo/Tobiko'
import { PlayIcon, PauseIcon } from '@heroicons/react/24/solid'
import "./Root.css";

export default function Root() {
  return (
    <>
      <Header></Header>
      <Main>
        <IDE></IDE>
      </Main>
      <Footer></Footer>
    </>
  );
}


function Header() {
  return (
    <header className='min-h-[3rem] px-2 py-1 flex justify-between items-center border-b-2 border-gray-600'>
      <LogoTobiko style={{ height: '32px' }} />
      <div>
        platform header
      </div>
    </header>
  )
}

function Main({ children }: { children: React.ReactNode }) {
  return (
    <main className='font-sans w-full h-full flex flex-col overflow-hidden'>
      {children}
    </main>
  )
}

function IDE() {
  return (
    <>
      <div className='w-full border-b-2 border-gray-600 flex justify-between min-h-[2rem]'>
        <div className='px-4'>Wursthall</div>
        <div className='px-4 flex'>
          <div className='px-4'>TobikoData/wursthall</div>
          <div className='px-4'>main</div>
        </div>
        <div className='px-4 w-full'>search</div>
        <div className='px-4 whitespace-nowrap'>
          <small>Envoirment: development</small>
        </div>
        <div className='px-4 flex items-center'>
          <div className='bg-gray-800 w-7 h-6 rounded flex justify-center items-center mr-2'>
            <PlayIcon className='w-4 h-4 text-primary-300' />
          </div>
          <div className='bg-gray-800 w-7 h-6 rounded flex justify-center items-center mr-2'>
            <PauseIcon className='w-4 h-4 text-primary-300' />
          </div>          
        </div>
        <div className='px-4'>users</div>
      </div>
      <div className='flex w-full h-full overflow-hidden'>
        <div className='min-w-[12rem] w-full max-w-[16rem] overflow-hidden overflow-y-auto border-r-2 border-gray-600'>
          <FolderTree />
        </div>
        <div className='h-full w-full flex flex-col overflow-hidden'>

          <div className='w-full overflow-hidden px-2 border-b-2 border-gray-600 min-h-[1.5rem] flex items-center'>
            <small className='inline-block cursor-pointer hover:text-gray-300'>audit</small>
            <small className='inline-block px-2'>/</small>
            <small className='inline-block cursor-pointer hover:text-gray-300'>items.sql</small>
          </div>

          <div className='min-w-[20rem] w-full h-full flex overflow-hidden border-b-2 border-gray-600'>
            <div className='w-full h-full flex flex-col border-r-2 border-gray-600'>
              <div className=' bg-gray-800 text-center flex'>
                <div className='p-1 min-w-[10rem] bg-gray-900 '>
                  Tab 1
                </div>
                <div className='p-1 min-w-[10rem] bg-gray-700 border-b-2 border-gray-600'>
                  Inactive
                </div>                
                <div className='p-1 border-b-2 border-gray-600 w-full'>
                  ide editor tabs
                </div>
              </div>
              <div className='w-full h-full flex'>
                <div className='h-full w-full p-2'>ide editor</div>
              </div>
              <div className='px-2 py-1 border-t-2 border-gray-600 flex justify-between'>
                <small>
                  ide editor footer
                </small>
                <small>
                  Total char count: 0
                </small>
              </div>
            </div>
            <div className='h-full min-w-[15%] w-full max-w-[25%] p-2'>Inspector</div>
          </div>
          <div className='w-full min-h-[10rem]'>
            <div className=' bg-gray-800 text-center flex'>
              <div className='p-1 min-w-[10rem] bg-gray-900 '>
                Table
              </div>
              <div className='p-1 min-w-[10rem] bg-gray-700 border-b-2 border-gray-600'>
                DAG
              </div>                
              <div className='p-1 border-b-2 border-gray-600 w-full'>
                ide preview tabs
              </div>
            </div>
            <div>ide preview</div>
          </div>
        </div>
      </div>
      <div className='p-1 border-t-2 border-gray-600'>ide footer</div>
    </>
  )
}

function Footer() {
  return (
    <footer className='px-2 py-1 text-xs flex justify-between border-t-2 border-gray-600'>
      <small>        
        {(new Date).getFullYear()}
      </small>
      platform footer
    </footer>
  )
}