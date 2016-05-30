{-# LANGUAGE ConstraintKinds, FlexibleContexts #-}
-- | Free List saved to disk
module Database.LowLevelDB.FreeList where


import System.IO
import Foreign.Storable
import Data.Default
import Control.Monad.IO.Class (liftIO, MonadIO)
import Foreign.Marshal.Alloc
import Control.Concurrent.MVar

-- | Free List structure
data FreeList a = FreeList
  {flSize :: Int -- ^ Record size
  , flHandle :: MVar Handle -- ^ File handle
  }

-- | Create a new free list from a file
newFileFreeList :: (Storable a,MonadIO m)
    => FilePath -- ^ File path
    -> a -- ^ For record size, can be undefined
    -> m (FreeList a)
newFileFreeList f a=  do
    h <-  liftIO $ openBinaryFile f ReadWriteMode
    initFreeList h a

-- | create a new free list from a handle
initFreeList :: (Storable a,MonadIO m)
    =>Handle -- ^ Handle
    -> a -- ^ For record size, can be undefined
    -> m (FreeList a)
initFreeList h a = liftIO $ do
  --at end
  hSeek h SeekFromEnd 0
  mv <- newMVar h
  return $ FreeList (fromIntegral $ sizeOf a) mv


-- | Close underlying handle, runs the callback, and return if we have still objects in the list
closeFreeList :: (Storable a,MonadIO m) => FreeList a -> m Bool
closeFreeList (FreeList _ mv)= liftIO $ withMVar mv $ \h->do
  i <- hTell h
  hClose h
  return $ i>0

-- | Add object to list
addToFreeList :: (Storable a,MonadIO m) => a -> FreeList a -> m ()
addToFreeList b (FreeList sz mv) = liftIO $ withMVar mv $ \h->
  allocaBytes sz $ \ptr-> do
            poke ptr b
            hPutBuf h ptr sz
  -- at end

-- | get object to list if list is non empty
getFromFreeList :: (Storable a,Eq a,Default a,MonadIO m) => FreeList a -> m (Maybe a)
getFromFreeList f@(FreeList sz mv) = liftIO $ withMVar mv $ \h-> do
  i <- hTell h
  if i>0
    then do
     let isz = fromIntegral sz
     r <- do
         hSeek h RelativeSeek (-isz)
         r<-allocaBytes (fromIntegral sz) $ \ptr-> hGetBuf h ptr sz >> peek ptr
         hSeek h RelativeSeek (-isz)
         return r
     if r /= def
        then return $ Just r
        else getFromFreeList f
    else return Nothing
