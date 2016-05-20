{-# LANGUAGE RecordWildCards,OverloadedStrings #-}
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
    , checkpoint
    , getModel
    , updateModel
    , getPropertyTypeID
    , getPropertyType
    , getNodeTypeID
    , getNodeType
    , getEdgeTypeID
    , getEdgeType
    , readNode
    , writeNode
    , deleteNode
    , readEdge
    , writeEdge
    , deleteEdge
    , readProperty
    , writeProperty
    , deleteProperty
    ) where

import Database.Graph.STMGraph.Constants
import Database.Graph.STMGraph.Types
import Database.Graph.STMGraph.LowLevel.MMapHandle

import Data.Default
import Data.Binary
import Data.Int
import Data.Maybe
import Foreign.Storable
import System.Directory
import System.FilePath
import System.IO
import qualified Data.ByteString  as BS
import Control.Concurrent
import Control.Concurrent.STM.TVar
import Control.Monad
import Control.Monad.STM
import Control.Concurrent.STM.TQueue
import qualified STMContainers.Map as SM
import qualified Data.Map.Strict                               as DM
import qualified Data.HashMap.Strict                               as HM
import qualified Data.Text as T
import Foreign.Marshal.Alloc
import Foreign.Ptr

open :: FilePath -> GraphSettings -> IO Database
open dir gs = do
    createDirectoryIfMissing True dir
    mv <- newEmptyMVar
    hs<- if gsUseMMap gs
                then
                  MMHandles
                    <$> getMMHandle nodeFile
                    <*> getMMHandle edgeFile
                    <*> getMMHandle propertyFile
                    <*> getMMHandle propertyValuesFile
                    <*> pure (dir </> modelFile)
                else
                 Handles
                    <$> getHandle nodeFile
                    <*> getHandle edgeFile
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
    startWriter hs tc mv = void $ forkFinally (withPtrs $ writer hs tc) (\_ -> putMVar mv ())


close :: Database -> IO ()
close Database{..} = do
    atomically $ writeTQueue dWrites ClosedDatabase
    takeMVar dWriterThread

checkpoint :: Database -> IO ()
checkpoint Database{..} = do
    mv <- newEmptyMVar
    atomically $ writeTQueue dWrites (Checkpoint mv)
    takeMVar mv


load :: Handles -> MVar() -> IO Database
load h mv = do
        exm <- doesFileExist (hModel h)
        mdl <- if exm then stringToModel <$> readFile (hModel h) else def
        db <- atomically $ do
            db<- newDatabase mv
            writeTVar (mdModel $ dMetadata db) mdl
            return db
        withPtrs $ \ptrs-> do
            loadNodes h db ptrs
            loadEdges h db ptrs
            loadProperties h mdl db ptrs
        return db

loadNodes :: Handles -> Database -> Ptrs ->  IO()
loadNodes h Database{..} Ptrs{..}=
    case h of
        Handles{..} -> foldAllGeneric hNodes ptrsNodes addNode addNodeID
        MMHandles{..}->foldAllGenericMM mhNodes (snd ptrsNodes) addNode addNodeID 10 -- FIXME, need to have max id
  where
        addNode (i,o) = atomically $ do
            SM.insert o i (gdNodes dData)
            modifyTVar'  (mdGenNodeID dMetadata) (\g->g{maxID=i})
        addNodeID i = atomically $ do
            freeID i 1 (mdGenNodeID dMetadata)
            return ()

loadEdges :: Handles -> Database -> Ptrs -> IO()
loadEdges h Database{..} Ptrs{..} =
     case h of
        Handles{..} -> foldAllGeneric hEdges ptrsEdges addEdge addEdgeID
        MMHandles{..}->foldAllGenericMM mhEdges (snd ptrsEdges) addEdge addEdgeID 10 -- FIXME, need to have max id
   where
        addEdge (i,o) = atomically $ do
            SM.insert o i (gdEdges dData)
            modifyTVar'  (mdGenEdgeID dMetadata) (\g->g{maxID=i})
        addEdgeID i = atomically $ do
            freeID i 1 (mdGenEdgeID dMetadata)
            return ()

loadProperties :: Handles -> Model -> Database -> Ptrs -> IO()
loadProperties h mdl Database{..} Ptrs{..} =
    case h of
        Handles{..} -> foldAllGeneric hProperties ptrsProperties addProperty addPropertyID
        MMHandles{..} -> foldAllGenericMM mhProperties (snd ptrsProperties) addProperty addPropertyID 10 -- FIXME, need to have max id
  where
        addProperty (i,o) = do
            let mt = HM.lookup (pType o) $ toName $ mPropertyTypes mdl
            case mt of
                Nothing -> void $ atomically $ freeID i 1 (mdGenPropertyID dMetadata)
                Just (_,t) -> do
                    v <- readPropertyValue h t (pOffset o) (pLength o)
                    atomically $ do
                        SM.insert (o,v) i (gdProperties dData)
                        modifyTVar'  (mdGenPropertyID dMetadata) (\g->g{maxID=i})
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
readPropertyValue MMHandles{..} dt off len = toValue dt <$> peekMMBS mhPropertyValues (fromIntegral off) (fromIntegral len)

-- | Read all binary Nodes from a given handle, generating their IDs from their offset
--foldAllGeneric
--  :: (Integral a, Eq b, Binary b, Default b)
--  => Handle -> Int64
--  -> ((a,b) -> IO ()) -- ^ callback on item
--  -> (a -> IO ()) -- ^ callback on empty
--  -> IO ()
--foldAllGeneric h sz f1 f2 = do
--  hSeek h AbsoluteSeek 0
--  go (fromIntegral sz) 0
--  where go isz a = do
--            bs <- BS.hGet h isz
--            unless (BS.null bs) $
--              do
--                let b = decode bs
--                    i = a + 1
--                if b == def
--                         then f2 i
--                         else f1 (i,b)
--                go isz i

foldAllGeneric
  :: (Integral a, Eq b, Storable b, Default b)
  => Handle -> (Ptr b,Int)
  -> ((a,b) -> IO ()) -- ^ callback on item
  -> (a -> IO ()) -- ^ callback on empty
  -> IO ()
foldAllGeneric h (ptr,sz) f1 f2 = do
  hSeek h AbsoluteSeek 0
  go 0
  where go a = do
            cnt <- hGetBuf h ptr sz
            unless (cnt<sz) $
              do
                b <- peek ptr
                let    i = a + 1
                if b == def
                         then f2 i
                         else f1 (i,b)
                go i

-- | Read all binary objects from a given mmap handle, generating their IDs from their offset
foldAllGenericMM
  :: (Integral a, Eq b, Storable b, Default b)
  => MMapHandle b -> Int
  -> ((a,b) -> IO ()) -- ^ callback on item
  -> (a -> IO ()) -- ^ callback on empty
  -> a
  -> IO ()
foldAllGenericMM h sz f1 f2 maxID =
  go 0
  where go a = do
            b <- peekMM h (fromIntegral a*sz)
            let i = a + 1
            if b == def
                     then f2 i
                     else f1 (i,b)
            if i >= maxID
              then return ()
              else  go i

writer :: Handles -> TQueue WriteEvent  -> Ptrs -> IO ()
writer hs@Handles{..} tc ptrs@Ptrs{..} = do
    e <- atomically $ readTQueue tc
    handle e
    unless (e == ClosedDatabase) $ writer hs tc ptrs
    where
        handle ClosedDatabase = do
            hClose hNodes
            hClose hEdges
            hClose hProperties
            hClose hPropertyValues
            return ()
        handle (Checkpoint mv) = do
            hFlush hNodes
            hFlush hEdges
            hFlush hProperties
            hFlush hPropertyValues
            putMVar mv ()
        handle (WrittenModel mdl) = writeFile hModel (modelToString mdl)
        handle (WrittenNode oid obj) = writeGeneric hNodes ptrsNodes oid obj
        handle (WrittenEdge rid rel) = writeGeneric hEdges ptrsEdges rid rel
        handle (WrittenProperty pid (pro,val)) = do
            writeGeneric hProperties ptrsProperties pid pro
            hSeek hPropertyValues AbsoluteSeek (toInteger $ pOffset pro)
            BS.hPut hPropertyValues val
        handle (DeletedNode oid) = writeGeneric hNodes ptrsNodes oid (def::Node)
        handle (DeletedEdge rid) = writeGeneric hEdges ptrsEdges rid (def::Edge)
        handle (DeletedProperty pid) = writeGeneric hProperties ptrsProperties pid (def::Property)
writer hs@MMHandles{..} tc ptrs@Ptrs{..} = do
    e <- atomically $ readTQueue tc
    handle e
    unless (e == ClosedDatabase) $ writer hs tc ptrs
    where
        handle ClosedDatabase = do
            closeMmap mhNodes
            closeMmap mhEdges
            closeMmap mhProperties
            closeMmap mhPropertyValues
            return ()
        handle (Checkpoint mv) = do
            putMVar mv ()
        handle (WrittenModel mdl) = writeFile hModel (modelToString mdl)
        handle (WrittenNode oid obj) = writeGenericMM mhNodes ptrsNodes oid obj
        handle (WrittenEdge rid rel) = writeGenericMM mhEdges ptrsEdges rid rel
        handle (WrittenProperty pid (pro,val)) = do
            writeGenericMM mhProperties ptrsProperties pid pro
            pokeMMBS mhPropertyValues val (fromIntegral $ pOffset pro)
        handle (DeletedNode oid) = writeGenericMM mhNodes ptrsNodes oid (def::Node)
        handle (DeletedEdge rid) = writeGenericMM mhEdges ptrsEdges rid (def::Edge)
        handle (DeletedProperty pid) = writeGenericMM mhProperties ptrsProperties pid (def::Property)

data Ptrs = Ptrs
    { ptrsNodes :: (Ptr Node, Int)
    , ptrsEdges :: (Ptr Edge, Int)
    , ptrsProperties :: (Ptr Property,Int)
    }

withPtrs :: (Ptrs -> IO a) -> IO a
withPtrs f= do
    let nsz = fromIntegral nodeSize
        esz = fromIntegral edgeSize
        psz = fromIntegral propertySize
    allocaBytes nsz $ \nptr->
        allocaBytes esz $ \eptr->
            allocaBytes psz $ \pptr->
                f $ Ptrs (nptr,nsz) (eptr,esz) (pptr,psz)

-- | Generic write operation: write the given binary using the given ID and record size
--writeGeneric :: (Integral a, Binary b) => Handle -> Int64 -> a -> b -> IO ()
--writeGeneric h sz a b =  do
--    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
--    BS.hPut h $ encode b

writeGeneric :: (Integral a, Storable b) => Handle -> (Ptr b, Int) -> a-> b -> IO ()
writeGeneric h (ptr,sz) a b =  do
    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
    poke ptr b
    hPutBuf h ptr sz

writeGenericMM :: (Integral a, Storable b) => MMapHandle b -> (Ptr b, Int) -> a-> b -> IO ()
writeGenericMM h (_,sz) a b =  do
    pokeMM h b (fromIntegral (a - 1) * sz)

updateModel ::  (Model -> Model) -> Database ->STM ()
updateModel upd db = do
    let tv = mdModel $ dMetadata db
    mdl <- readTVar tv
    let mdl2 = upd mdl
    when (mdl2 /= mdl) $ do
      writeTVar tv $! mdl2
      writeTQueue (dWrites db) (WrittenModel mdl2)

getModel :: Database -> STM Model
getModel db = readTVar $ mdModel $ dMetadata db

getPropertyTypeID :: Database -> (T.Text,DataType) -> STM PropertyTypeID
getPropertyTypeID db p = do
  let tv = mdModel $ dMetadata db
  mdl <- readTVar tv
  let mptid = HM.lookup p $ fromName $ mPropertyTypes mdl
  case mptid of
    Just ptid -> return ptid
    Nothing -> do
      let ns = toName $ mPropertyTypes mdl
          ptid = fromIntegral $ (HM.size ns)+1

      updateModel (\m->
          let pts2= addToLookup ptid p $ mPropertyTypes m
          in m{mPropertyTypes=pts2}
        ) db
      return ptid

getPropertyType :: Database -> PropertyTypeID -> STM (Maybe (T.Text,DataType))
getPropertyType db ptid = do
    let tv = mdModel $ dMetadata db
    mdl <- readTVar tv
    return $ HM.lookup ptid $ toName $ mPropertyTypes mdl

getNodeTypeID :: Database -> T.Text -> STM NodeTypeID
getNodeTypeID db n = do
  let tv = mdModel $ dMetadata db
  mdl <- readTVar tv
  let mptid = HM.lookup n $ fromName $ mNodeTypes mdl
  case mptid of
    Just ptid -> return ptid
    Nothing -> do
      let ns = toName $ mNodeTypes mdl
          ptid = fromIntegral $  (HM.size ns)+1

      updateModel (\m->
          let pts2= addToLookup ptid n $ mNodeTypes m
          in m{mNodeTypes=pts2}
        ) db
      return ptid

getNodeType :: Database -> NodeTypeID -> STM (Maybe T.Text)
getNodeType db ntid = do
    let tv = mdModel $ dMetadata db
    mdl <- readTVar tv
    return $ HM.lookup ntid $ toName $ mNodeTypes mdl

getEdgeTypeID :: Database -> T.Text -> STM NodeTypeID
getEdgeTypeID db n = do
  let tv = mdModel $ dMetadata db
  mdl <- readTVar tv
  let mptid = HM.lookup n $ fromName $ mEdgeTypes mdl
  case mptid of
    Just ptid -> return ptid
    Nothing -> do
      let ns = toName $ mEdgeTypes mdl
          ptid = fromIntegral $ (HM.size ns)+1

      updateModel (\m->
          let pts2= addToLookup ptid n $ mEdgeTypes m
          in m{mEdgeTypes=pts2}
        ) db
      return ptid

getEdgeType :: Database -> EdgeTypeID -> STM (Maybe T.Text)
getEdgeType db etid = do
    let tv = mdModel $ dMetadata db
    mdl <- readTVar tv
    return $ HM.lookup etid $ toName $ mEdgeTypes mdl

readNode :: Database ->NodeID -> STM Node
readNode db oid = fromMaybe def <$> SM.lookup oid (gdNodes $ dData db)

writeNode :: Database -> Maybe NodeID -> Node -> STM NodeID
writeNode db mid o = do
        i <- getID mid
        SM.insert o i $ gdNodes $ dData db
        writeTQueue (dWrites db) (WrittenNode i o)
        return i
    where
        getID (Just i)= return i
        getID _ = nextID (mdGenNodeID $ dMetadata db) 1

deleteNode :: Database -> NodeID -> STM ()
deleteNode db oid = do
    freeID oid 1 (mdGenNodeID $ dMetadata db)
    SM.delete oid $ gdNodes $ dData db
    writeTQueue (dWrites db) (DeletedNode oid)


readEdge :: Database ->EdgeID -> STM Edge
readEdge db oid = fromMaybe def <$> SM.lookup oid (gdEdges $ dData db)

writeEdge :: Database -> Maybe EdgeID -> Edge -> STM EdgeID
writeEdge db mid o = do
        i <- getID mid
        SM.insert o i $ gdEdges $ dData db
        writeTQueue (dWrites db) (WrittenEdge i o)
        return i
    where
        getID (Just i)= return i
        getID _ = nextID (mdGenEdgeID $ dMetadata db) 1

deleteEdge :: Database -> EdgeID -> STM ()
deleteEdge db oid = do
    freeID oid 1 (mdGenEdgeID $ dMetadata db)
    SM.delete oid $ gdEdges $ dData db
    writeTQueue (dWrites db) (DeletedEdge oid)

readProperty :: Database ->PropertyID -> STM (Property,PropertyValue)
readProperty db oid = fromMaybe def <$> SM.lookup oid (gdProperties $ dData db)

writeProperty :: Database -> Maybe PropertyID -> ((PropertyTypeID,PropertyID),PropertyValue) -> STM PropertyID
writeProperty db mid ((tid,next),v) = do
        let bs= toBin v
        let l = fromIntegral $ BS.length bs
        (i,off) <- getID mid l
        let p= Property tid next off l
        SM.insert (p,v) i $ gdProperties $ dData db
        writeTQueue (dWrites db) (WrittenProperty i (p,bs))
        return i
    where
        getID (Just i) l = do
            mo <- SM.lookup i $ gdProperties $ dData db
            case  mo of
                Just (pold,_) ->
                    if pLength pold==l
                        then return (i,pOffset pold)
                        else do
                            freeID (pOffset pold) (pLength pold) (mdGenPropertyOffset $ dMetadata db)
                            off <- getOff l
                            return (i,off)
                Nothing -> do
                    off <-  getOff l
                    return (i,off)
        getID _  l = do
            i <- nextID (mdGenPropertyID $ dMetadata db) 1
            off <- getOff l
            return (i,off)
        getOff = nextID (mdGenPropertyOffset $ dMetadata db)

deleteProperty :: Database -> PropertyID -> STM ()
deleteProperty db oid = do
    mo <- SM.lookup oid $ gdProperties $ dData db
    case  mo of
        Just (pold,_) ->
            freeID (pOffset pold) (pLength pold) (mdGenPropertyOffset $ dMetadata db)
        Nothing -> return ()
    freeID oid 1 (mdGenPropertyID $ dMetadata db)
    SM.delete oid $ gdProperties $ dData db
    writeTQueue (dWrites db) (DeletedProperty oid)
