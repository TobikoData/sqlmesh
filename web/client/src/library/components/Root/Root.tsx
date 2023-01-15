import FolderTree from '../FolderTree/FolderTree';
import LogoTobiko from '../Logo/Tobiko'
import "./Root.css";

export default function Root() {
  return (
    <>
      <header className='bg-gray-800 px-2 py-1'>
        <LogoTobiko style={{ height: '32px' }} />
      </header>
      <main className='w-full h-full flex overflow-hidden'>
        <div className='bg-gray-800 min-w-[4rem] max-w-[4rem] py-2'></div>
        <div className='bg-gray-700 min-w-[12rem] w-full max-w-[16rem] py-2 overflow-auto'>
          <FolderTree />
        </div>
        <div className='bg-gray-100 min-w-[20rem] w-full flex flex-col'>
          <div className='bg-gray-100 h-full py-2'></div>
          <div className='bg-gray-200 h-full max-h-[25%] py-2'></div>
        </div>
        <div className='bg-gray-800 min-w-[12rem] max-w-[12rem] py-2'></div>
      </main>
      <footer className='px-2 py-1 bg-gray-800 text-xs'>{(new Date).getFullYear()}</footer>
    </>
  );
}
