module Database.Graph.HGraphStorage.LowLevel.MMapHandle where

import System.IO.MMap
import Foreign.Storable
import Foreign.Ptr
import Data.Int
import Control.Concurrent.MVar
import qualified Data.ByteString.Lazy                   as BS
import Data.Word
import Control.Monad
import Control.Applicative

data MMapHandle a = MMapHandle
  { mhName     :: FilePath
  , mhHandle   :: !(MVar (MMapHandle_ a))
  }
 
data MMapHandle_ a = MMapHandle_
  { mhOrigOff  :: (Int64,Int)
  , mhPtr      :: Ptr a
  , mhRaw      :: Int
  , mhOff      :: Int
  , mhLen      :: Int
  }
  
  
openMmap :: (Storable a) => FilePath -> (Int64,Int) -> a -> IO (MMapHandle a)
openMmap fp offlen u1 = do
  h_ <- openMmap_ fp offlen u1
  mv <- newMVar h_
  return $ MMapHandle fp mv
  
openMmap_ :: (Storable a) => FilePath -> (Int64,Int) -> a -> IO (MMapHandle_ a)
openMmap_ fp offlen u1 = do
  (ptr,raw,off,len) <- mmapFilePtr fp ReadWriteEx $ Just offlen
  let al = alignment u1
  return $ MMapHandle_ offlen (alignPtr (plusPtr ptr off) al) raw off len
  
pokeMM :: (Storable a) => MMapHandle a -> a -> Int -> IO ()
pokeMM mm a sz = modifyMVar_ (mhHandle mm) p
  where 
    p h_ 
      | inBounds h_ (sz + sizeOf a) = do
          poke (plusPtr (mhPtr h_) sz) a
          return h_
      | otherwise = do
          h2_ <- extendMM h_ (mhName mm) (sz + sizeOf a)
          poke (plusPtr (mhPtr h2_) sz) a
          return h2_

pokeMMBS :: MMapHandle Word8 -> BS.ByteString -> Int -> IO ()
pokeMMBS mm a sz 
  | BS.null a = return ()
  | otherwise = modifyMVar_ (mhHandle mm) p
  where
    szw = sizeOf (0::Word8)
    up  = BS.unpack a
    full = sz + (szw * length up)
    fp ptr = void $ foldM (\p1 w-> do
            poke p1 w
            let p2 = plusPtr p1 szw
            return p2) (plusPtr ptr sz) up
    p h_
      | inBounds h_ full = do
          fp (mhPtr h_)
          return h_
      | otherwise = do
          h2_ <- extendMM h_ (mhName mm) full
          fp (mhPtr h2_)
          return h2_
   
extendMM :: (Storable a) => MMapHandle_ a -> FilePath -> Int -> IO (MMapHandle_ a)
extendMM h_ fp sz = do
    closeMM_ h_
    let (off,_)=mhOrigOff h_
    openMmap_ fp (off,sz*2) undefined
  
       
peekMM :: (Storable a) => MMapHandle a -> Int -> IO a
peekMM mm sz = modifyMVar (mhHandle mm) p
  where 
    p h_ 
      | inBounds h_ sz = do
        a<-peek (plusPtr (mhPtr h_) sz)
        return (h_,a)
      | otherwise = do
        h2_ <- extendMM h_ (mhName mm) sz
        a<-peek (plusPtr (mhPtr h2_) sz)
        return (h2_,a)
  
peekMMBS :: MMapHandle Word8 -> Int -> Int -> IO BS.ByteString
peekMMBS mm sz len = modifyMVar (mhHandle mm) p
  where 
    szw = sizeOf (0::Word8)
    full = sz + (szw * len)
    fp ptr = snd <$> foldM (\(p1,l) _ -> do 
          let p2 = plusPtr p1 (-szw)
          r<-peek p2 
          return (p2,r:l)) (plusPtr ptr full,[]) [1 .. len]
    p h_ 
      | inBounds h_ full = do
        a<-fp (mhPtr h_)
        return (h_,BS.pack a)
      | otherwise = do
        h2_ <- extendMM h_ (mhName mm) full
        a<-fp (mhPtr h2_)
        return (h2_,BS.pack a) 
  
 
closeMM :: MMapHandle a -> IO ()
closeMM mm = withMVar (mhHandle mm) closeMM_

closeMM_ :: MMapHandle_ a -> IO ()
closeMM_ mm = munmapFilePtr (mhPtr mm) (mhRaw mm)
  
inBounds :: MMapHandle_ a -> Int -> Bool
inBounds mm sz = sz + mhOff mm < mhRaw mm
  