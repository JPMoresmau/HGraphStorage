{-# LANGUAGE DeriveDataTypeable, StandaloneDeriving, ConstraintKinds, FlexibleInstances, MultiParamTypeClasses, TypeFamilies, UndecidableInstances, DeriveFunctor, GeneralizedNewtypeDeriving, RankNTypes, FlexibleContexts, RecordWildCards #-}
-- | Higher level API for reading and writing
module Database.Graph.HGraphStorage.API where

import Control.Applicative
import Control.Monad (MonadPlus, liftM, foldM, filterM, void, when, unless)
import Control.Monad.Base (MonadBase(..))
import Control.Monad.Fix (MonadFix)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (MonadTrans(lift))
import Control.Monad.Trans.Control ( MonadTransControl(..), MonadBaseControl(..)
                                   , ComposeSt, defaultLiftBaseWith
                                   , defaultRestoreM )
import Control.Arrow

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
import Database.Graph.HGraphStorage.Index as Idx
import Data.Int (Int16)
import System.Directory (doesFileExist)
import Data.Maybe (fromMaybe, catMaybes)
import Control.Exception.Lifted (throwIO)

-- | State for the monad
data GsData = GsData
  { gsHandles  :: Handles
  , gsModel    :: Model
  , gsDir      :: FilePath
  , gsSettings :: GraphSettings
  , gsIndexes  :: [(IndexInfo,Trie Int16 ObjectID)]
  }

-- | Index metadata
data IndexInfo = IndexInfo
  { iiName  :: T.Text
  , iiTypes :: [T.Text]
  , iiProps :: [T.Text]
  } deriving (Show,Read,Eq,Ord)


-- | Run a computation with the graph storage engine, storing the data in the given directory
withGraphStorage :: forall (m :: * -> *) a.
                      (R.MonadThrow m, MonadIO m,
                      MonadLogger m,
                       MonadBaseControl IO m) =>
                      FilePath -> GraphSettings -> GraphStorageT (R.ResourceT m) a -> m a
withGraphStorage dir gs act = R.runResourceT $ do
  (rk,hs) <- R.allocate (open dir gs) close
  model <- readModel hs
  res <- evalStateT (unIs loadIndexes) (GsData hs model dir gs [])
  R.release rk
  return res
  where 
    loadIndexes = do
      idxf <- indexFile
      ex <- liftIO $ doesFileExist idxf
      when ex $ do
        indexInfos <- liftM read $ liftIO $ readFile idxf
        mapM_ addIndex indexInfos
      act


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

-- | Get the current indices.
getIndices :: Monad m => GraphStorageT m [(IndexInfo,Trie Int16 ObjectID)]
getIndices = gsIndexes `liftM` Gs get


indexFile :: Monad m => GraphStorageT m FilePath
indexFile = do
  dir <- getDirectory
  return $ dir </> "indices"


-- | Create or replace an object.
createObject :: (GraphUsableMonad m) =>
                GraphObject (Maybe ObjectID)-> GraphStorageT m (GraphObject ObjectID)
createObject obj = do
  hs <- getHandles
  tid <- objectType $ goType obj
  toAdd <- removeOldValuesFromIndex obj (goID obj)
  --let props = filter (not . null . snd) $ DM.toList $ goProperties obj
  propId <- createProperties $ goProperties obj
  nid <- write hs (goID obj) (Object tid def def propId)
  insertNewValuesInIndex nid toAdd
  return $ obj {goID = nid}

-- | Replace an object.
updateObject :: (GraphUsableMonad m) =>
                GraphObject ObjectID -> GraphStorageT m (GraphObject ObjectID)
updateObject obj = do
  hs <- getHandles
  tid <- objectType $ goType obj
  toAdd <- removeOldValuesFromIndex obj (Just $ goID obj)
  --let props = filter (not . null . snd) $ DM.toList $ goProperties obj
  propId <- createProperties $ goProperties obj
  _ <- write hs (Just $ goID obj) (Object tid def def propId)
  insertNewValuesInIndex (goID obj) toAdd
  return obj
 
-- | Checks if there is a duplicate on any applicable index. Then remove obsolete values from the index, and generate the list of values to add
-- We'll only add the values once the object has been properly written, so we can have the ID of new objects.
removeOldValuesFromIndex :: (GraphUsableMonad m) => GraphObject a -> Maybe ObjectID -> GraphStorageT m [(T.Text,[Trie Int16 ObjectID],[PropertyValue])]
removeOldValuesFromIndex g mid = do
  idxMap <- indexMap g
  if DM.null idxMap
    then return []
    else do
      oldProps <- case mid of
        Nothing -> return DM.empty
        Just oid -> do
          hs <- getHandles
          obj <- readOne hs oid
          let pid = oFirstProperty obj
          listProperties pid
      let (toRem,toAdd) = foldr (removeIdx oldProps (goProperties g)) ([],[]) $ DM.assocs idxMap
      checkDuplicates mid toAdd
      liftIO $ mapM_ removeVals toRem
      return toAdd
  where
    removeIdx oldP newP (n,tries) (toRem,toAdd) = do
      let oldVs = fromMaybe [] $ DM.lookup n oldP
      let newVs = fromMaybe [] $ DM.lookup n newP
      case (oldVs,newVs) of
        ([],[]) -> (toRem,toAdd) -- no values, nothing to do
        (vs,[]) -> ((tries,vs):toRem,toAdd) -- no new values, remove old
        ([],ns) -> (toRem,(n,tries,ns):toAdd) -- new values, return ref
        (ovs,nvs)
          | ovs == nvs -> (toRem,toAdd) -- same values, nothing to do
          | otherwise  -> ((tries,ovs):toRem,(n,tries,nvs):toAdd)
    removeVals (tries,vs) = mapM_ (removeVal tries) vs
    removeVal tries v = mapM_ (delete (valueToIndex v)) tries
    
    
-- | Check if duplicates exist in index.
checkDuplicates :: (GraphUsableMonad m) => Maybe ObjectID -> [(T.Text,[Trie Int16 ObjectID],[PropertyValue])] -> GraphStorageT m ()
checkDuplicates mid toAdd = do
  dups <- liftIO 
              $   filter (not . null . snd)
                . map (second (filter (\ oid -> Just oid /= mid)))
              <$> mapM checkDups toAdd
  unless (null dups) $
        liftIO $ throwIO $ DuplicateIndexKey $ map fst dups            
  where
    checkDups (n,tries,vs) = do
      ids<-concat <$> mapM (checkDup tries) vs
      return (n,ids)
    checkDup tries v = catMaybes <$> mapM (Idx.lookup (valueToIndex v)) tries

      
-- | Insert new values in applicable indices.
insertNewValuesInIndex :: (GraphUsableMonad m) => ObjectID -> [(T.Text,[Trie Int16 ObjectID],[PropertyValue])] -> GraphStorageT m ()
insertNewValuesInIndex gid = liftIO . mapM_ addVals
  where
    addVals (_,tries,vs) = mapM_ (addVal tries) vs
    -- We should not have duplicates here, given removeOldValuesFromIndex
    addVal tries v = mapM_ (insert (valueToIndex v) gid) tries

 
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
  let pid = oFirstProperty obj
  pmap <- listProperties pid
  typeName <- getTypeName obj
  return $ GraphObject objId typeName pmap

-- | Get one object from its ID.
getObject :: (GraphUsableMonad m) =>
                ObjectID -> GraphStorageT m (GraphObject ObjectID)
getObject gid = populateObject gid =<< flip readOne gid =<< getHandles


-- | Get the type name for a given low level Object.
getTypeName :: (GraphUsableMonad m) => Object -> GraphStorageT m T.Text
getTypeName obj = do
  mdl <- getModel
  let otid = oType obj
  throwIfNothing (UnknownObjectType otid) $ DM.lookup otid $ toName $ mObjectTypes mdl


-- | (Internal) Build a property map by reading the property list.
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
                (GraphRelation RelationID ObjectID -> GraphStorageT m Bool) -> GraphStorageT m [GraphRelation RelationID ObjectID]
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
  typeName <- getTypeName obj
  _ <- removeOldValuesFromIndex (GraphObject oid typeName DM.empty) $ Just oid
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


-- | Add an index to be automatically managed.
addIndex :: (GraphUsableMonad m) => IndexInfo -> GraphStorageT m (Trie Int16 ObjectID)
addIndex ii@(IndexInfo idxName _ props) = do
  t <- createIndex idxName
  Gs (modify (\s@GsData{..} ->s{ gsIndexes = (ii,t):gsIndexes} ))
  idxf <- indexFile
  idxs <- getIndices
  liftIO $ writeFile idxf $ show $ map fst idxs
  getHandles >>= \hs->foldAll hs (fillIndex t) ()
  return t
  where
    fillIndex :: (GraphUsableMonad m) => Trie Int16 ObjectID -> () -> (ObjectID,Object) -> GraphStorageT m ()
    fillIndex t _ (gid,obj) = do
      typeName <- getTypeName obj
      when (isIndexApplicable ii typeName) $ do
        go <- populateObject gid obj
        let toAdd = map (\(k,v)-> (k,[t],v)) $ filter (\(k,_)->k `elem` props) $ DM.assocs $ goProperties go
        checkDuplicates (Just gid) toAdd
        insertNewValuesInIndex gid toAdd
        return ()
        

 -- | (Internal) Create an index.
createIndex :: forall k v m. (Binary k,Binary v,Default k,Default v,GraphUsableMonad m) =>
                T.Text -> GraphStorageT m (Trie k v)
createIndex idxName =  do
  dir <- getDirectory
  --trie <- liftIO $ newFileTrie $ dir </> T.unpack idxName
  (_,trie) <- R.allocate (liftIO $ newFileTrie $ dir </> T.unpack idxName) (hClose . trHandle)
  gs <- getSettings
  liftIO $ setBufferMode (trHandle trie) $ gsIndexBuffering gs
  return trie


-- | Get the indices to update, per property.
indexMap :: (GraphUsableMonad m) => GraphObject a -> GraphStorageT m (DM.Map T.Text [Trie Int16 ObjectID])
indexMap obj = do
  iis <- getIndices
  return $ foldr addPropIndex DM.empty iis
  where
    addPropIndex (ii,tr) dm
      | isIndexApplicable ii (goType obj) = foldr (\prop -> DM.insertWith (++) prop [tr]) dm $ iiProps ii
      | otherwise = dm
 

-- | Is the given index applicable to the given object type?
isIndexApplicable :: IndexInfo -> T.Text -> Bool
isIndexApplicable ii typ = let
  tps = iiTypes ii
  in null tps || typ `elem` tps 
