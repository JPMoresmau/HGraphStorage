{-# LANGUAGE DeriveDataTypeable, StandaloneDeriving, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, TypeFamilies, UndecidableInstances, DeriveFunctor, GeneralizedNewtypeDeriving, RankNTypes, FlexibleContexts #-}
-- | Higher level API for reading and writing
module Database.Graph.HGraphStorage.API where

import Control.Applicative
import Control.Monad (MonadPlus, liftM, foldM, filterM, void)
import Control.Monad.Base (MonadBase(..))
import Control.Monad.Fix (MonadFix)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (MonadTrans(lift))
import Control.Monad.Trans.Control ( MonadTransControl(..), MonadBaseControl(..)
                                   , ComposeSt, defaultLiftBaseWith
                                   , defaultRestoreM )

import qualified Data.Map as DM
import qualified Data.Text as T
import Data.Typeable
import Data.Binary (Binary)
import Data.Default

import Control.Monad.Logger

import qualified Control.Monad.Trans.Resource as R
import System.FilePath
import System.IO

import Database.Graph.HGraphStorage.FileOps
import Database.Graph.HGraphStorage.Types
import Control.Monad.Trans.State.Lazy
import Database.Graph.HGraphStorage.FreeList (addToFreeList)
import Database.Graph.HGraphStorage.Index

-- | State for the monad
data GsData = GsData
  { gsHandles  :: Handles
  , gsModel    :: Model
  , gsDir      :: FilePath
  , gsSettings :: GraphSettings
  } 


-- | Run a computation with the graph storage engine, storing the data in the given directory
withGraphStorage :: forall (m :: * -> *) a.
                      (R.MonadThrow m, MonadIO m,
                      MonadLogger m,
                       MonadBaseControl IO m) =>
                      FilePath -> GraphSettings -> GraphStorageT (R.ResourceT m) a -> m a
withGraphStorage dir gs (Gs act) = R.runResourceT $ do
  (rk,hs) <- R.allocate (open dir gs) close
  model <- readModel hs
  res <- evalStateT act (GsData hs model dir gs)
  R.release rk
  return res




-- | Our monad transformer
newtype GraphStorageT m a = Gs { unIs :: StateT GsData m a }
    deriving ( Functor, Applicative, Alternative, Monad
             , MonadFix, MonadPlus, MonadIO, MonadTrans
             , R.MonadThrow )

deriving instance R.MonadResource m => R.MonadResource (GraphStorageT m)

instance MonadBase b m => MonadBase b (GraphStorageT m) where
    liftBase = lift . liftBase

instance MonadTransControl GraphStorageT where
    newtype StT GraphStorageT a = GsStT { unGsStT :: StT (StateT GsData) a }
    liftWith f = Gs $ liftWith (\run -> f (liftM GsStT . run . unIs))
    restoreT = Gs . restoreT . liftM unGsStT

instance MonadBaseControl b m => MonadBaseControl b (GraphStorageT m) where
    newtype StM (GraphStorageT m) a = StMT {unStMT :: ComposeSt GraphStorageT m a}
    liftBaseWith = defaultLiftBaseWith StMT
    restoreM = defaultRestoreM unStMT

instance (MonadLogger m) => MonadLogger (GraphStorageT m) where
   monadLoggerLog loc src lvl msg=lift $ monadLoggerLog loc src lvl msg


--data Graph = Graph [GraphObject] [GraphRelation]
--  deriving (Show,Read,Eq,Ord,Typeable)

-- | An object with a type and properties
data GraphObject a = GraphObject
  { goID         :: a
  , goType       :: T.Text
  , goProperties :: DM.Map T.Text [PropertyValue]
  } deriving (Show,Read,Eq,Ord,Typeable)

-- | A relation between two objects, with a type and properties
data GraphRelation a b = GraphRelation
  { grID         :: a
  , grFrom       :: GraphObject b
  , grTo         :: GraphObject b
  , grType       :: T.Text
  , grProperties :: DM.Map T.Text [PropertyValue]
  } deriving (Show,Read,Eq,Ord,Typeable)
  


-- | Get the file handles.
getHandles :: Monad m => GraphStorageT m Handles
getHandles = gsHandles `liftM` Gs get

-- | Get the currently known model.
getModel :: Monad m => GraphStorageT m Model
getModel = gsModel `liftM` Gs get

-- | Get the currently known model.
getDirectory :: Monad m => GraphStorageT m FilePath
getDirectory = gsDir `liftM` Gs get


-- | Get the current settings.
getSettings :: Monad m => GraphStorageT m GraphSettings
getSettings = gsSettings `liftM` Gs get

-- | Create or replace an object
createObject :: (GraphUsableMonad m) =>
                GraphObject (Maybe ObjectID)-> GraphStorageT m (GraphObject ObjectID)
createObject obj = do
  hs <- getHandles
  tid <- objectType $ goType obj
  --let props = filter (not . null . snd) $ DM.toList $ goProperties obj
  propId <- createProperties $ goProperties obj
  nid <- write hs (goID obj) (Object tid def def propId)
  return $ obj {goID = nid}
  where 
  
-- | Create properties from map, returns the first ID in the chain
createProperties 
  :: (GraphUsableMonad m)
  => DM.Map T.Text [PropertyValue]
  -> GraphStorageT m PropertyID
createProperties = foldM addProps def . DM.toList
  where
    addProps nid (_,[]) = return nid 
    addProps nid (name,vs@(v:_)) = do
      let dt = valueType v
      ptid <- propertyType (name,dt)
      hs <- getHandles
      foldM (writeProperty hs ptid) nid vs

-- | filter objects
filterObjects :: (GraphUsableMonad m) =>
                (GraphObject ObjectID -> GraphStorageT m Bool) -> GraphStorageT m [GraphObject ObjectID]
filterObjects ft = filterM ft =<< (mapM (uncurry populateObject) =<< readAll =<< getHandles)
  
-- | (Internal) Fill an object with its properties
populateObject :: (GraphUsableMonad m) =>
                    ObjectID -> Object -> GraphStorageT m (GraphObject ObjectID)
populateObject objId obj = do
  mdl <- getModel
  let pid = oFirstProperty obj
  pmap <- listProperties pid
  let otid = oType obj
  typeName <- throwIfNothing (UnknownObjectType otid) $ DM.lookup otid $ toName $ mObjectTypes mdl
  return $ GraphObject objId typeName pmap

getObject :: (GraphUsableMonad m) =>
                ObjectID -> GraphStorageT m (GraphObject ObjectID)
getObject gid = populateObject gid =<< flip readOne gid =<< getHandles


-- | (Internal) Build a property map by reading the property list
listProperties
  :: (GraphUsableMonad m)
  => PropertyID
  -> GraphStorageT m (DM.Map T.Text [PropertyValue])
listProperties pid = do
  hs <- getHandles
  mdl <- getModel
  ps <- readProperties hs mdl def pid
  DM.fromList <$>
    mapM propName
      (DM.toList $ DM.fromListWith (++) $ map (\ (k, v) -> (k, [v])) ps)
  where
    propName (p,vs) = do
      mdl <- getModel
      let ptid = pType p
      (pName,_) <- throwIfNothing (UnknownPropertyType ptid) $ DM.lookup ptid $ toName $ mPropertyTypes mdl
      return (pName,vs)   
  

-- | Create a relation between two objects
createRelation :: (GraphUsableMonad m) =>
  GraphRelation (Maybe RelationID) (Maybe ObjectID) -> GraphStorageT m (GraphRelation RelationID ObjectID)
createRelation rel = do
  fromObj <- getObjectId $ grFrom rel
  toObj <- getObjectId $ grTo rel
  createRelation' $ GraphRelation (grID rel) fromObj toObj (grType rel) (grProperties rel)
  where
    getObjectId obj = case goID obj of
      Just i -> return obj{goID=i}
      Nothing -> createObject obj 
      
-- | Create a relation between two objects
createRelation' :: (GraphUsableMonad m) =>
  GraphRelation (Maybe RelationID) ObjectID -> GraphStorageT m (GraphRelation RelationID ObjectID)
createRelation' rel = do
  let fromObj = grFrom rel
  let fromId = goID fromObj
  fromTid <- objectType $ goType fromObj
  let toObj = grTo rel
  let toId = goID toObj
  toTid <- objectType $ goType toObj
  rid <- relationType $ grType rel
  propId <- createProperties $ grProperties rel
  hs <- getHandles
  fromTObj <- readOne hs fromId
  toTObj <- readOne hs toId
  nid <- write hs (grID rel) (Relation fromId fromTid toId toTid rid (oFirstFrom fromTObj) (oFirstTo toTObj) propId)
  _ <- write hs (Just fromId) fromTObj{oFirstFrom=nid}
  _ <- write hs (Just toId) toTObj{oFirstTo=nid}
  
  return rel{grID=nid,grFrom=fromObj,grTo=toObj}


-- | list relations matchinf a filter
filterRelations :: (GraphUsableMonad m) =>
                ((GraphRelation RelationID ObjectID)-> GraphStorageT m Bool) -> GraphStorageT m [GraphRelation RelationID ObjectID]
filterRelations ft = filterM ft =<< (mapM popProperties =<< readAll =<< getHandles)
  where 
    popProperties (relId,rel) = do
      mdl <- getModel
      let pid = rFirstProperty rel
      pmap <- listProperties pid
      let rtid = rType rel
      typeName <- throwIfNothing (UnknownRelationType rtid) $ DM.lookup rtid $ toName $ mRelationTypes mdl
      fromObj <- getObject $ rFrom rel
      toObj <- getObject $ rTo rel
      return $ GraphRelation relId fromObj toObj typeName pmap


-- | Delete a relation from the DB.
deleteRelation 
  :: (GraphUsableMonad m) 
  => RelationID
  -> GraphStorageT m ()
deleteRelation rid =
 void $ deleteRelation' rid True True

-- | (Internal) Delete a relation from the DB.
deleteRelation'
  :: (GraphUsableMonad m) 
  => RelationID
  -> Bool -- ^ Should we clean the origin object relation list? 
  -> Bool -- ^ Should we clean the target object relation list?
  -> GraphStorageT m [RelationID] -- ^ The next ids in the chain we didn't clean
deleteRelation' rid cleanFrom cleanTo = do
  hs <- getHandles
  rel <- readOne hs rid
  _ <- write hs (Just rid) (def::Relation)
  addToFreeList rid (hRelationFree hs)
  deleteProperties hs $ rFirstProperty rel
  
  let nextFrom = rFromNext rel
  ns1 <- if cleanFrom 
    then do
      let fromId = rFrom rel
      fromO <- readOne hs fromId
      let fstFromId = oFirstFrom fromO
      if fstFromId == rid
        then void $ write hs (Just fromId) fromO{oFirstFrom = nextFrom}
        else fixChain hs fstFromId rFromNext (\r -> r{rFromNext = nextFrom})
      return []
    else return [nextFrom]
  
  let nextTo = rToNext rel
  ns2 <- if cleanTo 
    then do
      let toId = rTo rel
      toO <- readOne hs toId
      let fstToId = oFirstTo toO
      if fstToId == rid
        then void $ write hs (Just toId) toO{oFirstTo = nextTo}
        else fixChain hs fstToId rToNext (\r -> r{rToNext = nextTo})
      return []
    else return [nextTo] 
  return $ filter (def /=) $ ns1 ++ ns2
  where
    fixChain _ crid _ _ | crid == def = return () 
    fixChain hs crid getNext setNext = do
      rel <- readOne hs crid
      let nid = getNext rel
      if nid == rid
        then void $ write hs (Just crid) $ setNext rel  
        else fixChain hs nid getNext setNext

-- | Delete an object
deleteObject 
  :: (GraphUsableMonad m) 
  => ObjectID
  -> GraphStorageT m ()
deleteObject oid = do
  hs <- getHandles
  obj <- readOne hs oid
  _ <- write hs (Just oid) (def::Object)
  addToFreeList oid (hObjectFree hs)
  cleanRef False True $ oFirstFrom obj
  cleanRef True False $ oFirstTo obj
  deleteProperties hs $ oFirstProperty obj
  
  where
    cleanRef _ _ rid | rid == def = return ()
    cleanRef cleanFrom cleanTo rid = do
      rids <- deleteRelation' rid cleanFrom cleanTo
      mapM_ (cleanRef  cleanFrom cleanTo) rids
  
-- | (Internal) Delete all properties in the list
deleteProperties
  :: (GraphUsableMonad m) 
  => Handles
  -> PropertyID
  -> GraphStorageT m ()
deleteProperties _ pid | pid == def = return ()
deleteProperties hs pid = do
  p <- readOne hs pid
  let next = pNext p
  _ <- write hs (Just pid) (def::Property)
  addToFreeList pid (hPropertyFree hs)
  -- TODO what about reclaiming the space of values?
  deleteProperties hs next

-- | (Internal) retrieve an object type id from its name (creating it if need be)
objectType :: (GraphUsableMonad m) 
  => T.Text -> GraphStorageT m ObjectTypeID
objectType typeName  = fetchType mObjectTypes
  (\mdl ots -> mdl {mObjectTypes = ots})
  typeName typeName
  ObjectType

-- | (Internal) retrieve a property type id from its name and data type (creating it if need be)
propertyType :: (GraphUsableMonad m) 
  => (T.Text,DataType) -> GraphStorageT m PropertyTypeID
propertyType t@(propName,dt) = fetchType mPropertyTypes
  (\mdl pts -> mdl {mPropertyTypes = pts})
  t propName
  (PropertyType $ dataTypeID dt)

-- | (Internal) retrieve an relation type id from its name (creating it if need be)
relationType :: (GraphUsableMonad m) 
  => T.Text -> GraphStorageT m RelationTypeID
relationType relationName = fetchType mRelationTypes
  (\mdl rts -> mdl {mRelationTypes = rts})
  relationName relationName
  RelationType

-- | (Internal) Fetch type helper
fetchType :: (GraphUsableMonad m, Ord k, GraphIdSerializable i v)
  => (Model -> Lookup i k)
  -> (Model -> Lookup i k -> Model)
  -> k
  -> T.Text
  -> (PropertyID -> v) 
  -> GraphStorageT m i
fetchType getM setM k name build = do
  mdl <- getModel
  let mid = DM.lookup k $ fromName $ getM mdl
  case mid of
    Just i  -> return i
    Nothing -> do
      hs <- getHandles
      pid <- writeProperty hs namePropertyID def $ PVText name
      newid <- write hs Nothing $ build pid
      let pts = addToLookup newid k $ getM mdl
          mdl2 = setM mdl pts
      Gs $ modify (\s -> s{gsModel=mdl2})
      return newid  

 -- | create an index
createIndex :: forall k v m. (Binary k,Binary v,Default k,Default v,GraphUsableMonad m) =>
                T.Text -> GraphStorageT m (Trie k v)
createIndex idxName =  do
  dir <- getDirectory
  --trie <- liftIO $ newFileTrie $ dir </> T.unpack idxName
  (_,trie) <- R.allocate (liftIO $ newFileTrie $ dir </> T.unpack idxName) (hClose . trHandle)
  gs <- getSettings
  liftIO $ setBufferMode (trHandle trie) $ gsIndexBuffering gs
  return trie
