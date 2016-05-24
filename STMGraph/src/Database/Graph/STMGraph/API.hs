{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.Graph.STMGraph.API
  ( open
  , close
  , checkpoint
  , Database

  , STMGraphT
  , GraphSettings(..)
  , withDatabaseIO
  , withDatabase
  , getDatabase

  , nbNodes
  , nbEdges
  , addNode
  , removeNode
  , nodeProperties
  , addEdge
  , removeEdge
  , edgeProperties
  , NameValue(..)
  , traverseGraph
  , Traversal(..)
  , Result(..)
  , Info(..)
  )where

import Database.Graph.STMGraph.APITypes
import Database.Graph.STMGraph.Types
import Database.Graph.STMGraph.Raw

import Control.Monad
import Control.Monad.STM
import Control.Monad.IO.Class (MonadIO, liftIO)

import Data.Monoid
import Data.Default
import qualified Data.Text as T
import qualified ListT
import qualified STMContainers.Map as SM
import Control.Monad.Trans.State.Strict
import qualified Control.Monad.Trans.Resource as R

import Control.Concurrent.STM.TVar
import Data.Int

nbNodes :: (Monad m,MonadIO m) => STMGraphT m Int64
nbNodes = withDB $ \db-> cNodes <$> readTVar (mdCounts $ dMetadata db)

nbEdges :: (Monad m,MonadIO m) => STMGraphT m Int64
nbEdges  = withDB $ \db-> cEdges <$> readTVar (mdCounts $ dMetadata db)

toPropertyValue :: NameValue -> (T.Text,PropertyValue)
toPropertyValue (TextP n v)=(n,PVText v)
toPropertyValue (IntP n v)=(n,PVInteger v)
toPropertyValue (BinP n v)=(n,PVBinary v)
toPropertyValue (JsonP n v)=(n,PVJSON v)

toNameValue :: (T.Text,PropertyValue) -> NameValue
toNameValue (n,PVText v) = TextP n v
toNameValue (n,PVInteger v) = IntP n v
toNameValue (n,PVBinary v) = BinP n v
toNameValue (n,PVJSON v) = JsonP n v

withDatabaseIO ::   (Monad m,MonadIO m,R.MonadThrow m,
                       R.MonadBaseControl IO m) =>  FilePath -> GraphSettings -> STMGraphT (R.ResourceT m) a -> m a
withDatabaseIO path gs f = R.runResourceT $ do
--    bracket
--        (open path gs)
--        close
--        (\db->withDatabase db f)
  (rk,db) <- R.allocate (open path gs) close
  res <- withDatabase db f
  liftIO $ checkpoint db
  R.release rk
  return res

withDatabase :: (Monad m) => Database -> STMGraphT m a -> m a
withDatabase db f = evalStateT (unIs f) db

withDB :: (Monad m,MonadIO m) => (Database -> STM a) -> STMGraphT m a
withDB f = do
    db <- getDatabase
    liftIO $ atomically $ f db

getDatabase ::  (Monad m,MonadIO m) => STMGraphT m Database
getDatabase = Gs get

addNode :: (Monad m,MonadIO m) => T.Text -> [NameValue] -> STMGraphT m NodeID
addNode tp props = withDB $ \db-> do
          pid <- createProperties db props
          tid <- getNodeTypeID db tp
          let obj=Node tid def def pid
          writeNode db Nothing obj

removeNode :: (Monad m,MonadIO m) => NodeID -> STMGraphT m ()
removeNode nid = withDB $ \db-> do
    n <- readNode db nid
    deleteProperties db $ nFirstProperty n
    removeEdges db CleanTo $ nFirstFrom n
    removeEdges db CleanFrom $ nFirstTo n
    deleteNode db nid
  where
    removeEdges db rem eid
     | eid == def = return ()
     | otherwise = removeEdges db rem =<< removeEdge' db eid rem


nodeProperties :: (Monad m,MonadIO m) => NodeID -> ([NameValue] -> STM [NameValue]) -> STMGraphT m [NameValue]
nodeProperties nid upd = withDB $ \db-> do
    n <- readNode db nid
    oldVals <- getNodeProperties db n
    newVals <- upd oldVals
    when (newVals /= oldVals) $ do
        deleteProperties db $ nFirstProperty n
        pid <- createProperties db newVals
        void $ writeNode db (Just nid) (n{nFirstProperty=pid})
    return newVals

addEdge :: (Monad m,MonadIO m) => NodeID -> T.Text -> [NameValue] -> NodeID -> STMGraphT m EdgeID
addEdge from tp props to =  withDB $ \db-> do
          fromN<-readNode db from
          toN <- readNode db to
          addEdge' db (from,fromN) tp props (to,toN)

addEdge' :: Database -> (NodeID,Node) -> T.Text -> [NameValue] -> (NodeID,Node) -> STM EdgeID
addEdge' db (from,fn) tp props (to,tn) = do
  pid <- createProperties db props
  tid <- getEdgeTypeID db tp
  let edg = Edge from (nType fn) to (nType tn) tid (nFirstFrom fn) (nFirstTo tn) pid
  eid <- writeEdge db Nothing edg
  writeNode db (Just from) (fn{nFirstFrom=eid})
  writeNode db (Just to) (tn{nFirstTo=eid})
  return eid

removeEdge :: (Monad m,MonadIO m) => EdgeID -> STMGraphT m ()
removeEdge eid = withDB $ \db-> void $ removeEdge' db eid CleanBoth

removeEdge' :: Database -> EdgeID -> EdgeRemoval -> STM EdgeID
removeEdge' db eid rem
  | eid == def = return def
  | otherwise = do
    e <- readEdge db eid
    deleteProperties db $ eFirstProperty e
    let nextFrom = eFromNext e
    pidF <- if shouldCleanFrom rem
      then do
        let fr = eFrom e
        n <- readNode db fr
        let fstId = nFirstFrom n
        if fstId == eid
          then void $ writeNode db (Just fr) n{nFirstFrom = nextFrom}
          else fixChain fstId eFromNext (\r -> r{eFromNext = nextFrom})
        return def
      else return nextFrom
    let nextTo = eToNext e
    pidT <- if shouldCleanTo rem
       then do
        let fr = eTo e
        n <- readNode db fr
        let fstId = nFirstTo n
        if fstId == eid
          then void $ writeNode db (Just fr) n{nFirstTo = nextTo}
          else fixChain fstId eToNext (\r -> r{eToNext = nextTo})
        return def
      else return nextTo
    deleteEdge db eid
    return $ getCleaned rem pidF pidT
  where
    fixChain crid getNext setNext
        | crid == def = return ()
        | otherwise = do
              rel <- readEdge db crid
              let nid = getNext rel
              if nid == eid
                then void $ writeEdge db (Just crid) $ setNext rel
                else fixChain nid getNext setNext

edgeProperties :: (Monad m,MonadIO m) => EdgeID -> ([NameValue] -> STM [NameValue]) -> STMGraphT m [NameValue]
edgeProperties eid upd = withDB $ \db->do
    e <- readEdge db eid
    oldVals <- getEdgeProperties db e
    newVals <- upd oldVals
    when (newVals /= oldVals) $ do
        deleteProperties db $ eFirstProperty e
        pid <- createProperties db newVals
        void $ writeEdge db (Just eid) (e{eFirstProperty=pid})
    return newVals

createProperties :: Database -> [NameValue] -> STM PropertyID
createProperties db = foldM createProperty def . map toPropertyValue
  where
    createProperty nid (name,value) = do
      let dt=valueType value
      ptid <- getPropertyTypeID db (name,dt)
      writeProperty db Nothing ((ptid,nid),value)

deleteProperties :: Database -> PropertyID -> STM()
deleteProperties db pid
    | pid == def =  return ()
    | otherwise = do
        (p,_) <- readProperty db pid
        deleteProperty db pid
        let nid = pNext p
        deleteProperties db nid


getNodeProperties :: Database -> Node -> STM [NameValue]
getNodeProperties db n = getProperties db $ nFirstProperty n

getEdgeProperties :: Database -> Edge -> STM [NameValue]
getEdgeProperties db e = getProperties db $ eFirstProperty e

getProperties :: Database -> PropertyID -> STM [NameValue]
getProperties db fp = getNamedProperties db fp Nothing

getNamedProperties :: Database -> PropertyID -> Maybe [T.Text] -> STM [NameValue]
getNamedProperties db fp mfns = readProp fp []
    where
      readProp fid ls
        | fid == def = return ls
        | otherwise = do
            (p,v) <- readProperty db fid
            when (pNext p == fid) $ error "loop!"
            mn <- getPropertyType db (pType p)
            case mn of
                Nothing -> error $ "unknown property type:" <> show (pType p)
                Just n -> readProp (pNext p) $
                    if isFiltered mfns n
                        then toNameValue (fst n,v):ls
                        else ls
      isFiltered Nothing _ = True
      isFiltered (Just fns) (n,_)=n  `elem` fns

nodeHasNamedValue :: Database -> Node -> NameValue -> STM Bool
nodeHasNamedValue db n nv = do
  nvs <- getNodeProperties db n
  return $ nv `elem` nvs


edgeHasNamedValue :: Database -> Edge -> NameValue -> STM Bool
edgeHasNamedValue db e nv = do
  nvs <- getEdgeProperties db e
  return $ nv `elem` nvs

traverseGraph :: (Monad m,MonadIO m) => Traversal -> STMGraphT m Result
traverseGraph t = stateToResult <$> withDB (\db -> doTraverse db SUnknown t)


doTraverse :: Database -> TState -> Traversal -> STM TState
doTraverse _ SEmpty _ = return SEmpty
doTraverse _ r Noop = return r
doTraverse db st (Composed ts) = foldM (doTraverse db) st ts
doTraverse _ _ Ns = return SAllNodes
doTraverse _ _ Es = return SAllEdges
doTraverse db SAllNodes (NID nids) = do
    ns <- filter (\(_,n)-> n/= def) <$> mapM (\nid ->
        do
            n<-readNode db nid
            return (nid,n)) nids
    return $ nodesIfAny ns
doTraverse _ (SNodes ns) (NID nids) =  do
    let fs = filter (\(nid,_)->nid `elem` nids) ns
    return $ nodesIfAny fs
doTraverse _ _ (NID _) = return SEmpty
doTraverse db SAllEdges (EID eids) = do
    ns <- filter (\(_,e)-> e/= def) <$> mapM (\eid ->
        do
            e<-readEdge db eid
            return (eid,e)) eids
    return $ edgesIfAny ns
doTraverse _ (SEdges es) (EID eids) =  do
    let fs = filter (\(eid,_)->eid `elem` eids) es
    return $ edgesIfAny fs
doTraverse _ _ (EID _) = return SEmpty
doTraverse db SAllNodes (Has nv) = do
  let st = SM.stream $ gdNodes $ dData db
  fs <- ListT.fold (\l (nid,n)->do
    has <- nodeHasNamedValue db n nv
    return $ if has then (nid,n):l else l) [] st
  return $ nodesIfAny fs
doTraverse db (SNodes ns) (Has nv) = do
    fs <- filterM (\(_,n)->nodeHasNamedValue db n nv) ns
    return $ nodesIfAny fs
doTraverse db SAllEdges (Has nv) = do
  let st = SM.stream $ gdEdges $ dData db
  fs <- ListT.fold (\l (eid,e)->do
    has <- edgeHasNamedValue db e nv
    return $ if has then (eid,e):l else l) [] st
  return $ edgesIfAny fs
doTraverse db (SEdges es) (Has nv) = do
    fs <- filterM (\(_,e)->edgeHasNamedValue db e nv) es
    return $ edgesIfAny fs
doTraverse db t (Values vs) = readProperties db t (Just vs)
doTraverse db t AllValues = readProperties db t Nothing
doTraverse db s (Out eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        es <- doTraverse db s (OutE eTypes)
        edgesToNodes eTo db es
doTraverse db s (In eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        es <- doTraverse db s (InE eTypes)
        edgesToNodes eFrom db es
doTraverse db r (Both eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        inEs <- doTraverse db r (In eTypes)
        outEs <- doTraverse db r (Out eTypes)
        return $ inEs <> outEs
doTraverse db SAllNodes (OutE eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        let st = SM.stream $ gdNodes $ dData db
        es <- ListT.fold (\l (_,n)->do
            es<-readOutEdges db (nFirstFrom n) eTypes
            return $ es:l) [] st
        return $ SEdges $ concat es
doTraverse db (SNodes ns) (OutE eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        es <- mapM (\(_,n)->readOutEdges db (nFirstFrom n) eTypes) ns
        return $ SEdges $ concat es
doTraverse db SAllNodes (InE eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        let st = SM.stream $ gdNodes $ dData db
        es <- ListT.fold (\l (_,n)->do
            es<-readInEdges db (nFirstTo n) eTypes
            return $ es:l) [] st
        return $ SEdges $ concat es
doTraverse db (SNodes ns) (InE eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        es <- mapM (\(_,n)->readInEdges db (nFirstTo n) eTypes) ns
        return $ SEdges $ concat es
doTraverse db r (BothE eTypes)
    | null eTypes = return SEmpty
    | otherwise   = do
        inEs <- doTraverse db r (InE eTypes)
        outEs <- doTraverse db r (OutE eTypes)
        return $ inEs <> outEs
doTraverse _ r t = return $ SError $ "Traversal not handled: " <> T.pack (show t) <> " in state: " <> T.pack (show r)

readProperties :: Database -> TState -> Maybe [T.Text] -> STM TState
readProperties db SAllNodes mvs = do
    let st = SM.stream $ gdNodes $ dData db
    getPropNames mvs <$> ListT.fold (\l (nid,n)-> do
      ps<-addNodeInfo db (nid,n) =<< getNamedProperties db (nFirstProperty n) mvs
      return (ps:l)
      ) [] st
readProperties db (SNodes ns) mvs =
    getPropNames mvs <$> mapM (\(nid,n)->addNodeInfo db (nid,n) =<< getNamedProperties db (nFirstProperty n) mvs) ns
readProperties db SAllEdges mvs = do
    let st = SM.stream $ gdEdges $ dData db
    getPropNames mvs <$> ListT.fold (\l (eid,e)-> do
      ps<-addEdgeInfo db (eid,e) =<< getNamedProperties db (eFirstProperty e) mvs
      return (ps:l)
      ) [] st
readProperties db (SEdges es) mvs =
    getPropNames mvs <$> mapM (\(eid,e)->addEdgeInfo db (eid,e) =<<getNamedProperties db (eFirstProperty e) mvs) es
readProperties _ st _ = return $ SError $ "Properties not handled in state: " <> T.pack (show st)

edgesToNodes :: (Edge -> NodeID) -> Database -> TState -> STM TState
edgesToNodes f db (SEdges es) =do
    fs <- mapM (\(_,e)->do
                let nid=f e
                n <- readNode db nid
                return (nid,n)) es
    return $ nodesIfAny fs
edgesToNodes _ _ s = error $ "edgesToNodes: Unexpected state:" <> show s

addNodeInfo :: Database -> (NodeID,Node) -> [NameValue] -> STM Info
addNodeInfo db (nid,n) nvs = do
    mt <- getNodeType db (nType n)
    case mt of
        Nothing -> error $ "addNodeInfo: unknown node type:" <> show (nType n)
        Just t -> return $ NodeInfo nid t nvs


addEdgeInfo :: Database -> (EdgeID,Edge) -> [NameValue] -> STM Info
addEdgeInfo db (eid,e) nvs = do
    mt <- getEdgeType db (eType e)
    case mt of
        Nothing -> error $ "addEdgeInfo: unknown edge type:" <> show (eType e)
        Just t -> return $ EdgeInfo eid t nvs


getPropNames :: Maybe [T.Text]  -> [Info] -> TState
getPropNames (Just vs) nvs = SProperties vs nvs
getPropNames _ nvs = SProperties (ordNub $ concatMap (map name . properties) nvs) nvs

nodesIfAny :: [(NodeID,Node)] -> TState
nodesIfAny [] = SEmpty
nodesIfAny ns = SNodes ns

edgesIfAny :: [(EdgeID,Edge)] -> TState
edgesIfAny [] = SEmpty
edgesIfAny es = SEdges es

readOutEdges :: Database -> EdgeID -> [T.Text] ->STM [(EdgeID,Edge)]
readOutEdges = readEdges eFromNext

readInEdges :: Database -> EdgeID -> [T.Text] ->STM [(EdgeID,Edge)]
readInEdges = readEdges eToNext

readEdges :: (Edge -> EdgeID) -> Database -> EdgeID -> [T.Text] ->STM [(EdgeID,Edge)]
readEdges next db eid eTypes =go eid []
    where
         go eid' ls
            | def == eid' = return ls
            | otherwise = do
            e <- readEdge db eid'
            mt <- getEdgeType db (eType e)
            case mt of
                Nothing -> error $ "unknown edge type:" <> show (eType e)
                Just t ->
                    if "*" `elem` eTypes || t `elem` eTypes
                        then go (next e) ((eid',e):ls)
                        else go (next e) ls

