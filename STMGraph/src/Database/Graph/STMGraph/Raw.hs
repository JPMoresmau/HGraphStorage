{-# LANGUAGE RecordWildCards #-}
-----------------------------------------------------------------------------
--
-- Module      :  Database.Graph.STMGraph.Raw
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

module Database.Graph.STMGraph.Raw
    ( open
    , close
    , getModel
    , updateModel
    ) where

import Database.Graph.STMGraph.Constants
import Database.Graph.STMGraph.Types
import Database.Graph.STMGraph.LowLevel.MMapHandle

import Data.Default
import Data.Binary
import Data.Int
import Foreign.Storable
import System.Directory
import System.FilePath
import System.IO
import qualified Data.ByteString.Lazy  as BS
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.STM
import Control.Concurrent.STM.TChan
import qualified STMContainers.Map as SM
import qualified STMContainers.Set as SS
import qualified Data.Map                               as DM

open :: FilePath -> GraphSettings -> IO Database
open dir gs = do
    createDirectoryIfMissing True dir
--    if gsUseMMap gs
--        then
--          MMHandles
--            <$> getMMHandle objectFile
--            <*> getMMHandle relationFile
--            <*> getMMHandle propertyFile
--            <*> getMMHandle propertyValuesFile
--            <*> pure (dir </> modelFile)
--        else
    mv <- newEmptyMVar
    hs <-     Handles
            <$> getHandle objectFile
            <*> getHandle relationFile
            <*> getHandle propertyFile
            <*> getHandle propertyValuesFile
            <*> pure (dir </> modelFile)
    db <- load hs mv
    startWriter hs (dWrites db) mv
    return db
  where
    getHandle :: FilePath -> IO Handle
    getHandle name = do
      let f = dir </> name
      h <- openBinaryFile f ReadWriteMode
      setBufferMode h $ gsMainBuffering gs
      return h
    getMMHandle :: (Default a,Storable a) => FilePath -> IO (MMapHandle a)
    getMMHandle name = openMmap (dir </> name) (0,4096) def
    startWriter hs tc mv = forkFinally (writer hs tc) (\_ -> putMVar mv ())

close :: Database -> IO ()
close Database{..} = do
    atomically $ writeTChan dWrites ClosedDatabase
    takeMVar dWriterThread

load :: Handles -> MVar() -> IO Database
load h@Handles{..} mv = do
        exm <- doesFileExist hModel
        mdl <- if exm then stringToModel <$> readFile hModel else def
        db <- atomically $ do
            db<- newDatabase mv
            writeTVar (mdModel $ dMetadata db) mdl
            return db
        loadObjects h db
        loadRelations h db
        loadProperties h mdl db
        return db

loadObjects :: Handles -> Database -> IO()
loadObjects Handles{..} Database{..} = foldAllGeneric hObjects objectSize addObject addObjectID
    where
        addObject (i,o) = atomically $ do
            SM.insert o i (gdObjects dData)
            writeTVar (maxID $ mdGenObjectID dMetadata) i
        addObjectID i = atomically $ do
            freeID i 1 (mdGenObjectID dMetadata)
            return ()

loadRelations :: Handles -> Database -> IO()
loadRelations Handles{..} Database{..} = void $ foldAllGeneric hRelations relationSize addRelation addRelationID
    where
        addRelation (i,o) = atomically $ do
            SM.insert o i (gdRelations dData)
            writeTVar (maxID $ mdGenRelationID dMetadata) i
        addRelationID i = atomically $ do
            freeID i 1 (mdGenRelationID dMetadata)
            return ()

loadProperties :: Handles -> Model -> Database -> IO()
loadProperties h@Handles{..} mdl Database{..} = void $ foldAllGeneric hProperties propertySize addProperty addPropertyID
    where
        addProperty (i,o) = do
            let mt = DM.lookup (pType o) $ toName $ mPropertyTypes mdl
            case mt of
                Nothing -> void $ atomically $ freeID i 1 (mdGenPropertyID dMetadata)
                Just (n,t) -> do
                    v <- readPropertyValue h t (pOffset o) (pLength o)
                    atomically $ do
                        SM.insert (o,v) i (gdProperties dData)
                        writeTVar (maxID $ mdGenPropertyID dMetadata) i
        addPropertyID i = atomically $ do
            freeID i 1 (mdGenPropertyID dMetadata)
            return ()

-- | Set the buffer mode on the given handle, if provided.
setBufferMode :: Handle -> Maybe BufferMode -> IO()
setBufferMode _ Nothing = return ()
setBufferMode h (Just bm) = hSetBuffering h bm

-- | Read a property value given an offset and length
readPropertyValue :: Handles -> DataType -> PropertyValueOffset -> PropertyValueLength -> IO PropertyValue
readPropertyValue Handles{..} dt off len = do
  let h = hPropertyValues
  hSeek h AbsoluteSeek (fromIntegral off)
  toValue dt <$> BS.hGet h (fromIntegral len)

-- | Read all binary objects from a given handle, generating their IDs from their offset
foldAllGeneric
  :: (Integral a, Eq b, Binary b, Default b)
  => Handle -> Int64
  -> ((a,b) -> IO ()) -- ^ callback on item
  -> (a -> IO ()) -- ^ callback on empty
  -> IO ()
foldAllGeneric h sz f1 f2 = do
  hSeek h AbsoluteSeek 0
  go (fromIntegral sz) 0
  where go isz a = do
            bs <- BS.hGet h isz
            unless (BS.null bs) $
              do
                let b = decode bs
                    i = a + 1
                if b == def
                         then f2 i
                         else f1 (i,b)
                go isz i

writer :: Handles -> TChan WriteEvent -> IO ()
writer hs@Handles{..} tc = do
    e <- atomically $ readTChan tc
    handle e
    unless (e == ClosedDatabase) $ writer hs tc
    where
        handle ClosedDatabase = do
            hClose hObjects
            hClose hRelations
            hClose hProperties
            hClose hPropertyValues
            return ()
        handle (WrittenModel mdl) = writeFile hModel (modelToString mdl)
        handle (WrittenObject oid obj) = writeGeneric hObjects objectSize oid obj
        handle (WrittenRelation rid rel) = writeGeneric hRelations relationSize rid rel
        handle (WrittenProperty pid pro val) = do
            writeGeneric hProperties propertySize pid pro
            hSeek hPropertyValues AbsoluteSeek (toInteger $ pOffset pro)
            BS.hPut hPropertyValues (toBin val)
        handle (DeletedObject oid) = writeGeneric hObjects objectSize oid (def::Object)
        handle (DeletedRelation rid) = writeGeneric hRelations relationSize rid (def::Relation)
        handle (DeletedProperty pid) = writeGeneric hProperties propertySize pid (def::Property)


-- | Generic write operation: write the given binary using the given ID and record size
writeGeneric :: (Integral a,Binary a,Default a, Binary b) => Handle -> Int64 -> a -> b -> IO ()
writeGeneric h sz a b =  do
    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
    BS.hPut h $ encode b

updateModel ::  (Model -> Model) -> Database ->STM ()
updateModel upd db = do
    let tv = mdModel $ dMetadata db
    mdl <- readTVar tv
    let mdl2 = upd mdl
    writeTVar tv $! mdl2
    writeTChan (dWrites db) (WrittenModel mdl2)

getModel :: Database -> STM Model
getModel db = readTVar $ mdModel $ dMetadata db

readObject :: Database ->ObjectID -> STM Object
readObject db oid = do
    mo <- SM.lookup oid $ gdObjects $ dData db
    case  mo of
        Just o -> return o
        Nothing -> retry

writeObject :: Database -> Maybe ObjectID -> Object -> STM ObjectID
writeObject db mid o = do
        i <- getID mid
        SM.insert o i $ gdObjects $ dData db
        writeTChan (dWrites db) (WrittenObject i o)
        return i
    where
        getID (Just i)= return i
        getID _ = nextID (mdGenObjectID $ dMetadata db) 1

