{-# LANGUAGE ConstraintKinds, FlexibleContexts #-}
-----------------------------------------------------------------------------
--
-- Module      :  Database.LowLevelDB.FreeList
-- Copyright   :
-- License     :  AllRightsReserved
--
-- Maintainer  :
-- Stability   :
-- Portability :
--
-- |
--
-----------------------------------------------------------------------------

module Database.LowLevelDB.FreeList where


import System.IO
import Foreign.Storable
import Data.Default
import Control.Monad.IO.Class (liftIO, MonadIO)
import Foreign.Marshal.Alloc

-- | Free List structure
data FreeList a m = FreeList
  {flSize :: Int -- ^ Record size
  , flHandle :: Handle -- ^ File handle
  , flOnEmptyClose :: m() -- ^ What to do on close if no record
  }

newFileFreeList :: (Storable a,MonadIO m) => FilePath -> a -> m() -> m (FreeList a m)
newFileFreeList f a onClose=  do
    h <-  liftIO $ openBinaryFile f ReadWriteMode
    initFreeList h a onClose

-- | position handle
initFreeList :: (Storable a,MonadIO m) =>Handle -> a -> m() -> m (FreeList a m)
initFreeList h a onClose = liftIO $ do
  --at end
  hSeek h SeekFromEnd 0
  return $ FreeList (fromIntegral $ sizeOf a) h onClose


-- | Close underlying handle and return if we have still objects in the list
closeFreeList :: (Storable a,MonadIO m) => FreeList a m -> m Bool
closeFreeList (FreeList _ h onClose)= do
  i <- liftIO $ hTell h
  liftIO $ hClose h
  if i>0
    then return True
    else do
      onClose
      return False

-- | Add object to list
addToFreeList :: (Storable a,MonadIO m) => a -> FreeList a m1 -> m ()
addToFreeList b (FreeList sz h _) = liftIO $
  allocaBytes sz $ \ptr-> do
            poke ptr b
            hPutBuf h ptr sz
  -- at end

-- | get object to list if list is non empty
getFromFreeList :: (Storable a,Eq a,Default a,MonadIO m) => FreeList a m1 -> m (Maybe a)
getFromFreeList f@(FreeList sz h _) = do
  i <- liftIO $ hTell h
  if i>0
    then do
     let isz = fromIntegral sz
     r <- liftIO $ do
         hSeek h RelativeSeek (-isz)
         r<-allocaBytes (fromIntegral sz) $ \ptr-> hGetBuf h ptr sz >> peek ptr
         hSeek h RelativeSeek (-isz)
         return r
     if r /= def
        then return $ Just r
        else getFromFreeList f
    else return Nothing
