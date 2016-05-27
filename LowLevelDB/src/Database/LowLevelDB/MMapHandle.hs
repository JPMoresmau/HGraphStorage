-- | Wraps the usage of mmap, handling extending the mapped area size if necessary
module Database.LowLevelDB.MMapHandle
  ( MMapHandle
  , openMmap
  , closeMmap
  , peekMM
  , pokeMM
  , peekMMBS
  , peekMMBSL
  , peekWord8s
  , pokeMMBS
  , pokeMMBSL
  , pokeWord8s
  )
  where

import System.IO.MMap
import Foreign.Storable
import Foreign.Ptr
import Data.Int
import Control.Concurrent.MVar
import qualified Data.ByteString                          as BS
import qualified Data.ByteString.Lazy                  as BSL
import Data.Word
import Control.Monad

import Control.Monad.IO.Class (liftIO, MonadIO)

-- | A Handle to a Mmapped file
data MMapHandle a = MMapHandle
  { mhName     :: FilePath -- ^ The file being mmapped
  , mhHandle   :: !(MVar (MMapHandle_ a)) -- ^ The underlying pointer information
  }

-- | The underlying pointer information
data MMapHandle_ a = MMapHandle_
  { mhOrigOff  :: (Int64,Int) -- ^ original offset and length
  , mhPtr      :: Ptr a -- ^ Pointer
  , mhRaw      :: Int -- ^ Raw length
  , mhOff      :: Int -- ^ Offset
  }

-- | Open a MMapHandle on a file
openMmap
  :: (Storable a,MonadIO m)
  => FilePath -- ^ File name
  -> (Int64,Int) -- ^ Offset/length
  -> a  -- ^ needed for alignment, not used
  -> m (MMapHandle a)
openMmap fp offlen u1 =  liftIO $ do
  h_ <- openMmap_ fp offlen u1
  mv <- newMVar h_
  return $ MMapHandle fp mv

-- | Open an internal MMapHandle on a file
openMmap_ :: (Storable a) => FilePath -> (Int64,Int) -> a -> IO (MMapHandle_ a)
openMmap_ fp offlen u1 = do
  (ptr,raw,off,_) <- mmapFilePtr fp ReadWriteEx $ Just offlen
  let al = alignment u1
  return $ MMapHandle_ offlen (alignPtr (plusPtr ptr off) al) raw off


-- | Write a value into the handle at the given offset
pokeMM :: (Storable a,MonadIO m) => MMapHandle a -> a -> Int -> m ()
pokeMM mm a sz = liftIO $ modifyMVar_ (mhHandle mm) p
  where
    p h_
      | inBounds h_ (sz + sizeOf a) = do
          poke (plusPtr (mhPtr h_) sz) a
          return h_
      | otherwise = do
          h2_ <- extendMM h_ (mhName mm) (sz + sizeOf a)
          poke (plusPtr (mhPtr h2_) sz) a
          return h2_

-- | Write a strict bytetring into the handle at the given offset
pokeMMBS ::(MonadIO m) => MMapHandle Word8 -> BS.ByteString -> Int -> m ()
pokeMMBS mm a =pokeWord8s mm (BS.unpack a)


-- | Write a lazy bytetring into the handle at the given offset
pokeMMBSL :: (MonadIO m)=> MMapHandle Word8 -> BSL.ByteString -> Int -> m ()
pokeMMBSL mm a = pokeWord8s mm (BSL.unpack a)

-- | Write an array of words into the handle at the given offset
pokeWord8s :: (MonadIO m) => MMapHandle Word8 -> [Word8]
    -> Int -- ^ Offset
    -> m ()
pokeWord8s mm ws off
  | null ws = return ()
  | otherwise = liftIO $ modifyMVar_ (mhHandle mm) p
  where
    szw = sizeOf (0::Word8)
    full = off + (szw * length ws)
    fp ptr = void $ foldM (\p1 w-> do
            poke p1 w
            let p2 = plusPtr p1 szw
            return p2) (plusPtr ptr off) ws
    p h_
      | inBounds h_ full = do
          fp (mhPtr h_)
          return h_
      | otherwise = do
          h2_ <- extendMM h_ (mhName mm) full
          fp (mhPtr h2_)
          return h2_

-- | Extend the pointer range to include the given index
extendMM :: (Storable a) => MMapHandle_ a -> FilePath
    -> Int -- ^ The size
    -> IO (MMapHandle_ a)
extendMM h_ fp sz = do
    closeMM_ h_
    let (off,_)=mhOrigOff h_
    openMmap_ fp (off,sz*2) undefined

-- | Read the value at the given offset
peekMM :: (Storable a,MonadIO m) => MMapHandle a
    -> Int -- ^ Offset
    -> m a
peekMM mm off = liftIO $ modifyMVar (mhHandle mm) p
  where
    p h_
      | inBounds h_ off = do
        a<-peek (plusPtr (mhPtr h_) off)
        return (h_,a)
      | otherwise = do
        h2_ <- extendMM h_ (mhName mm) off
        a<-peek (plusPtr (mhPtr h2_) off)
        return (h2_,a)

-- | Read a strict byte string at the given offset
peekMMBS :: (MonadIO m) => MMapHandle Word8
    -> Int -- ^ Offset
    -> Int -- ^ Length
    -> m BS.ByteString
peekMMBS mm off len = BS.pack <$> peekWord8s mm off len

-- | Read a lazy byte string at the given offset
peekMMBSL :: (MonadIO m)=>MMapHandle Word8
    -> Int -- ^ Offset
    -> Int -- ^ Length
    -> m BSL.ByteString
peekMMBSL mm off len = BSL.pack <$> peekWord8s mm off len

-- | Read an array of words at the given offset
peekWord8s :: (MonadIO m)=>MMapHandle Word8
    -> Int -- ^ Offset
    -> Int -- ^ Length
    -> m [Word8]
peekWord8s mm off len = liftIO $ modifyMVar (mhHandle mm) p
  where
    szw = sizeOf (0::Word8)
    full = off + (szw * len)
    fp ptr = snd <$> foldM (\(p1,l) _ -> do
          let p2 = plusPtr p1 (-szw)
          r<-peek p2
          return (p2,r:l)) (plusPtr ptr full,[]) [1 .. len]
    p h_
      | inBounds h_ full = do
        a<-fp (mhPtr h_)
        return (h_,a)
      | otherwise = do
        h2_ <- extendMM h_ (mhName mm) full
        a<-fp (mhPtr h2_)
        return (h2_,a)

-- | Close the MMapHandle
closeMmap :: (MonadIO m) => MMapHandle a -> m ()
closeMmap mm = liftIO $ withMVar (mhHandle mm) closeMM_

-- | Close the internal handle
closeMM_ :: MMapHandle_ a -> IO ()
closeMM_ mm = munmapFilePtr (mhPtr mm) (mhRaw mm)

-- | is the index in bound?
inBounds :: MMapHandle_ a -> Int -> Bool
inBounds mm sz = sz + mhOff mm < mhRaw mm
