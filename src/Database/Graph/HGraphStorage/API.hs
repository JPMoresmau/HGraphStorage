{-# LANGUAGE DeriveDataTypeable, StandaloneDeriving, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, TypeFamilies, UndecidableInstances, DeriveFunctor, GeneralizedNewtypeDeriving, RankNTypes, FlexibleContexts #-}
-- | Higher level API for reading and writing
module Database.Graph.HGraphStorage.API where

import Control.Applicative
import Control.Monad (MonadPlus, liftM, foldM, filterM)
import Control.Monad.Base (MonadBase(..))
import Control.Monad.Fix (MonadFix)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Class (MonadTrans(lift))
import Control.Monad.Trans.Control ( MonadTransControl(..), MonadBaseControl(..)
                                   , ComposeSt, defaultLiftBaseWith
                                   , defaultRestoreM )

import qualified Data.Map as DM
import qualified Data.Text as T
import Data.Typeable

import Control.Monad.Logger

import qualified Control.Monad.Trans.Resource as R

import Database.Graph.HGraphStorage.FileOps
import Database.Graph.HGraphStorage.Types
import Data.Default (def)
import Control.Monad.Trans.State.Lazy


-- | State for the monad
data GsData = GsData
  { gsHandles :: Handles
  , gsModel   :: Model
  } 


-- | Run a computation with the graph storage engine, storing the data in the given directory
withGraphStorage :: forall (m :: * -> *) a.
                      (R.MonadThrow m, R.MonadUnsafeIO m, MonadIO m,
                      MonadLogger m,
                       MonadBaseControl IO m) =>
                      FilePath -> GraphStorageT (R.ResourceT m) a -> m a
withGraphStorage dir (Gs act) = R.runResourceT $ do
  (rk,hs) <- R.allocate (open dir) close
  model <- readModel hs
  res <- evalStateT act (GsData hs model)
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


data Graph = Graph [GraphObject] [GraphRelation]
  deriving (Show,Read,Eq,Ord,Typeable)

-- | An object with a type and properties
data GraphObject = GraphObject
  { goID         :: Maybe ObjectID
  , goType       :: T.Text
  , goProperties :: DM.Map T.Text [PropertyValue]
  } deriving (Show,Read,Eq,Ord,Typeable)

-- | A relation between two objects, with a type and properties
data GraphRelation = GraphRelation
  { grID         :: Maybe RelationID
  , grFrom       :: GraphObject
  , grTo         :: GraphObject
  , grType       :: T.Text
  , grProperties :: DM.Map T.Text [PropertyValue]
  } deriving (Show,Read,Eq,Ord,Typeable)
  


-- | Get the file handles.
getHandles :: Monad m => GraphStorageT m Handles
getHandles = gsHandles `liftM` Gs get

-- | Get the currently known model.
getModel :: Monad m => GraphStorageT m Model
getModel = gsModel `liftM` Gs get

-- | Create or replace an object
createObject :: (GraphUsableMonad m) =>
                GraphObject -> GraphStorageT m GraphObject
createObject obj = do
  hs <- getHandles
  tid <- objectType $ goType obj
  --let props = filter (not . null . snd) $ DM.toList $ goProperties obj
  propId <- createProperties $ goProperties obj
  nid <- write hs (goID obj) (Object tid def def propId)
  return $ obj {goID = Just nid}
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
                (GraphObject -> GraphStorageT m Bool) -> GraphStorageT m [GraphObject]
filterObjects ft = filterM ft =<< (mapM (uncurry populateObject) =<< readAll =<< getHandles)
  
populateObject :: (GraphUsableMonad m) =>
                    ObjectID -> Object -> GraphStorageT m GraphObject
populateObject objId obj = do
  mdl <- getModel
  let pid = oFirstProperty obj
  pmap <- listProperties pid
  let otid = oType obj
  typeName <- throwIfNothing (UnknownObjectType otid) $ DM.lookup otid $ toName $ mObjectTypes mdl
  return $ GraphObject (Just objId) typeName pmap

getObject :: (GraphUsableMonad m) =>
                ObjectID -> GraphStorageT m GraphObject
getObject gid = populateObject gid =<< flip readOne gid =<< getHandles



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
  


createRelation :: (GraphUsableMonad m) =>
  GraphRelation -> GraphStorageT m GraphRelation
createRelation rel = do
  (fromId,fromObj) <- getObjectId $ grFrom rel
  fromTid <- objectType $ goType fromObj
  (toId,toObj) <- getObjectId $ grTo rel
  toTid <- objectType $ goType toObj
  rid <- relationType $ grType rel
  propId <- createProperties $ grProperties rel
  hs <- getHandles
  fromTObj <- readOne hs fromId
  toTObj <- readOne hs toId
  -- TODO next, previous, links to from obj, etc...
  nid <- write hs (grID rel) (Relation fromId fromTid toId toTid rid (oFirstFrom fromTObj) (oFirstTo toTObj) propId)
  _ <- write hs (Just fromId) fromTObj{oFirstFrom=nid}
  _ <- write hs (Just toId) toTObj{oFirstTo=nid}
  
  return rel{grID=Just nid,grFrom=fromObj,grTo=toObj}
  where
    getObjectId obj = case goID obj of
      Just i -> return (i,obj)
      Nothing -> getObjectId =<< createObject obj 
      

filterRelations :: (GraphUsableMonad m) =>
                (GraphRelation -> GraphStorageT m Bool) -> GraphStorageT m [GraphRelation]
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
      return $ GraphRelation (Just relId) fromObj toObj typeName pmap


objectType :: (GraphUsableMonad m) 
  => T.Text -> GraphStorageT m ObjectTypeID
objectType typeName  = fetchType mObjectTypes
  (\mdl ots -> mdl {mObjectTypes = ots})
  typeName typeName
  ObjectType

propertyType :: (GraphUsableMonad m) 
  => (T.Text,DataType) -> GraphStorageT m PropertyTypeID
propertyType t@(propName,dt) = fetchType mPropertyTypes
  (\mdl pts -> mdl {mPropertyTypes = pts})
  t propName
  (PropertyType $ dataTypeID dt)

relationType :: (GraphUsableMonad m) 
  => T.Text -> GraphStorageT m RelationTypeID
relationType relationName = fetchType mRelationTypes
  (\mdl rts -> mdl {mRelationTypes = rts})
  relationName relationName
  RelationType


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

 