{-# LANGUAGE RecordWildCards #-}
module Database.Graph.TGraph.Data
     where

import Database.Graph.TGraph.Constants
import Database.Graph.TGraph.Model
import Database.Graph.TGraph.Types
import Database.Graph.TGraph.LowLevel.MMapHandle
import Database.Graph.TGraph.LowLevel.FreeList
import Data.TCache
import Data.Binary
import Control.Monad
import qualified Data.ByteString.Lazy                   as BS
import           Data.Typeable
import           GHC.Generics                           (Generic)
import qualified Data.Text as T
import qualified Data.Map as DM
import           Data.Default
import           Foreign.Storable

import System.IO
import System.Directory
import System.FilePath

-- | Open all the file handles.
open :: FilePath -> IO Handles
open dir = do
  createDirectoryIfMissing True dir
  MMHandles
    <$> -- getMMHandle objectFile
    -- <*> getFreeList objectFile (def::ObjectID)
    -- <*> getMMHandle relationFile
    -- <*> getFreeList relationFile (def::RelationID)
        getMMHandle propertyFile
    <*> getFreeList propertyFile (def::PropertyID)
    <*> getMMHandle propertyValuesFile
  --  <*> getMMHandle maxIDsFile
  where
    getMMHandle :: (Default a,Storable a) => FilePath -> IO (MMapHandle a)
    getMMHandle name = openMmap (dir </> name) (0,4096) def
    getFreeList :: (Binary a) => FilePath -> a -> IO (FreeList a)
    getFreeList name d = do
      let f = dir </> freePrefix ++ name
      h<- openBinaryFile f ReadWriteMode
      --setBufferMode h $ gsFreeBuffering gs
      initFreeList (fromIntegral $ binLength d) h (do
          ex <- doesFileExist f
          when ex $ removeFile f)

-- | Set the buffer mode on the given handle, if provided.
setBufferMode :: Handle -> Maybe BufferMode -> IO()
setBufferMode _ Nothing = return ()
setBufferMode h (Just bm) = hSetBuffering h bm

-- | Close all the file handles
close :: Handles -> IO ()
close MMHandles{..} = do
  syncCache
--  closeMmap mhObjects
--  _  <- closeFreeList hObjectFree
--  closeMmap mhRelations
--  _ <- closeFreeList hRelationFree
  closeMmap mhProperties
  _ <- closeFreeList hPropertyFree
  closeMmap mhPropertyValues

-- | Write a property, knowing the next one in the chain
writeProperty :: PropertyTypeID -> Maybe (DBRef Property) -> PropertyValue -> MetaData -> STM (DBRef Property)
writeProperty ptid nextid pValue md = do
  let maxRef = mdMaxIDs md
  maxIds <- getDefRef maxRef
  let pid = miProperty maxIds + 1
      offset = miValueOffset maxIds
      valueLength = BS.length $ toBin pValue
  writeDBRef maxRef maxIds{miProperty=pid,miValueOffset=offset+valueLength}
  newDBRef $ Property pid ptid nextid pValue offset valueLength
