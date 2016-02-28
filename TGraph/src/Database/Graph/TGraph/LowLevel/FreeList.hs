{-# LANGUAGE ConstraintKinds, FlexibleContexts #-}
-- | Free list management
module Database.Graph.TGraph.LowLevel.FreeList where

import System.IO
import Data.Binary
import Data.Default
import qualified Data.ByteString.Lazy  as BS
import Control.Monad.IO.Class (liftIO, MonadIO)

-- | Free List structure
data FreeList a = FreeList
  { flSize :: Integer -- ^ Record size
  , flHandle :: Handle -- ^ File handle
  , flOnEmptyClose :: IO() -- ^ What to do on close if no record
  }

-- | position handle
initFreeList :: (Binary a,MonadIO m) => Integer -> Handle -> IO() -> m (FreeList a)
initFreeList sz h onClose = liftIO $ do
  --at end
  hSeek h SeekFromEnd 0
  return $ FreeList sz h onClose


-- | Close underlying handle and return if we have still objects in the list
closeFreeList :: (Binary a,MonadIO m) => FreeList a -> m Bool
closeFreeList (FreeList sz h onClose)= liftIO $ do
  i <- liftIO $ hTell h
  hClose h
  if i>=sz
    then return True
    else do
      onClose
      return False

-- | Add object to list
addToFreeList :: (Binary a,MonadIO m) => a -> FreeList a -> m ()
addToFreeList b (FreeList _ h _) = liftIO $
  BS.hPut h $ encode b
  -- at end

-- | get object to list if list is non empty
getFromFreeList :: (Binary a,Eq a,Default a,MonadIO m) => FreeList a -> m (Maybe a)
getFromFreeList f@(FreeList sz h _) = do
  i <- liftIO $ hTell h
  if i>=sz
    then do
      bs <- liftIO $ do
        hSeek h RelativeSeek (-sz)
        let isz = fromIntegral sz
        bs <- BS.hGet h isz
        hSeek h RelativeSeek (-sz)
        return bs
      if BS.null bs
        then return Nothing
        else do
          let r = decode bs
          if r /= def
            then return $ Just r
            else getFromFreeList f
    else return Nothing
