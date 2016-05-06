{-# LANGUAGE DeriveDataTypeable, OverloadedStrings #-}
module Database.Graph.STMGraph.API
  ( nbNodes
  , nbEdges
  , addNode
  , removeNode
  , addEdge
  , removeEdge
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

import Data.Monoid
import Data.Default
import Data.List
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BS
import qualified Data.Aeson as A
import Data.Typeable
import qualified ListT as ListT
import qualified STMContainers.Map as SM
import qualified Data.Set as S

nbNodes :: Database -> STM Int
nbNodes db = ListT.fold (\c _->return (c+1)) 0 $ SM.stream (gdNodes $ dData db)

nbEdges :: Database -> STM Int
nbEdges db = ListT.fold (\c _->return (c+1)) 0 $ SM.stream (gdEdges $ dData db)

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

addNode :: Database -> T.Text -> [NameValue] -> STM NodeID
addNode db tp props = do
  pid <- createProperties db props
  tid <- getNodeTypeID db tp
  let obj=Node tid def def pid
  writeNode db Nothing obj

removeNode ::Database -> NodeID -> STM ()
removeNode db nid = do
    n <- readNode db nid
    deleteProperties db $ nFirstProperty n
    removeEdges CleanTo $ nFirstFrom n
    removeEdges CleanFrom $ nFirstTo n
    deleteNode db nid
  where
    removeEdges rem eid
     | eid == def = return ()
     | otherwise = removeEdges rem =<< removeEdge' db eid rem

addEdge :: Database -> NodeID -> T.Text -> [NameValue] -> NodeID -> STM EdgeID
addEdge db from tp props to = do
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

removeEdge :: Database -> EdgeID -> STM ()
removeEdge db eid = void $ removeEdge' db eid CleanBoth

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
            mn <- getPropertyType db (pType p)
            case mn of
                Nothing -> error $ "unknown property type:" <> show (pType p)
                Just n -> readProp (pNext p) $
                    if isFiltered mfns n
                        then (toNameValue (fst n,v):ls)
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

traverseGraph :: Database -> Traversal -> STM Result
traverseGraph db = doTraverse db Unknown

doTraverse :: Database -> Result -> Traversal -> STM Result
doTraverse db Empty _ = return Empty
doTraverse db r Noop = return r
doTraverse db st (Composed ts) = foldM (doTraverse db) st ts
doTraverse db _ Ns = return AllNodes
doTraverse db _ Es = return AllEdges
doTraverse db AllNodes (NID nids) = do
    ns <- filter (\(_,n)-> n/= def) <$> mapM (\nid ->
        do
            n<-readNode db nid
            return (nid,n)) nids
    return $ nodesIfAny ns
doTraverse db (Nodes ns) (NID nids) =  do
    let fs = filter (\(nid,_)->nid `elem` nids) ns
    return $ nodesIfAny fs
doTraverse db _ (NID _) = return Empty
doTraverse db AllEdges (EID eids) = do
    ns <- filter (\(_,e)-> e/= def) <$> mapM (\eid ->
        do
            e<-readEdge db eid
            return (eid,e)) eids
    return $ edgesIfAny ns
doTraverse db (Edges es) (EID eids) =  do
    let fs = filter (\(eid,_)->eid `elem` eids) es
    return $ edgesIfAny fs
doTraverse db _ (EID _) = return Empty
doTraverse db AllNodes (Has nv) = do
  let st = SM.stream $ gdNodes $ dData db
  fs <- ListT.fold (\l (nid,n)->do
    has <- nodeHasNamedValue db n nv
    return $ if has then (nid,n):l else l) [] st
  return $ nodesIfAny fs
doTraverse db (Nodes ns) (Has nv) = do
    fs <- filterM (\(_,n)->nodeHasNamedValue db n nv) ns
    return $ nodesIfAny fs
doTraverse db AllEdges (Has nv) = do
  let st = SM.stream $ gdEdges $ dData db
  fs <- ListT.fold (\l (eid,e)->do
    has <- edgeHasNamedValue db e nv
    return $ if has then (eid,e):l else l) [] st
  return $ edgesIfAny fs
doTraverse db (Edges es) (Has nv) = do
    fs <- filterM (\(_,e)->edgeHasNamedValue db e nv) es
    return $ edgesIfAny fs
doTraverse db t (Values vs) = readProperties db t (Just vs)
doTraverse db t AllValues = readProperties db t Nothing
doTraverse _ r t = return $ Error $ "Traversal not handled: " <> T.pack (show t) <> " in state: " <> T.pack (show r)

readProperties :: Database -> Result -> Maybe [T.Text] -> STM Result
readProperties db AllNodes mvs = do
    let st = SM.stream $ gdNodes $ dData db
    getPropNames mvs <$> ListT.fold (\l (nid,n)-> do
      ps<-addNodeInfo db (nid,n) =<< getNamedProperties db (nFirstProperty n) mvs
      return (ps:l)
      ) [] st
readProperties db (Nodes ns) mvs =
    getPropNames mvs <$> mapM (\(nid,n)->addNodeInfo db (nid,n) =<< getNamedProperties db (nFirstProperty n) mvs) ns
readProperties db AllEdges mvs = do
    let st = SM.stream $ gdEdges $ dData db
    getPropNames mvs <$> ListT.fold (\l (eid,e)-> do
      ps<-addEdgeInfo db (eid,e) =<< getNamedProperties db (eFirstProperty e) mvs
      return (ps:l)
      ) [] st
readProperties db (Edges es) mvs =
    getPropNames mvs <$> mapM (\(eid,e)->addEdgeInfo db (eid,e) =<<getNamedProperties db (eFirstProperty e) mvs) es

addNodeInfo :: Database -> (NodeID,Node) -> [NameValue] -> STM Info
addNodeInfo db (nid,n) nvs = do
    mt <- getNodeType db (nType n)
    case mt of
        Nothing -> error $ "unknown node type:" <> show (nType n)
        Just t -> return $ NodeInfo nid t nvs


addEdgeInfo :: Database -> (EdgeID,Edge) -> [NameValue] -> STM Info
addEdgeInfo db (eid,e) nvs = do
    mt <- getEdgeType db (eType e)
    case mt of
        Nothing -> error $ "unknown edge type:" <> show (eType e)
        Just t -> return $ EdgeInfo eid t nvs


getPropNames :: Maybe [T.Text]  -> [Info] -> Result
getPropNames (Just vs) nvs = Properties vs nvs
getPropNames _ nvs = Properties (ordNub $ concatMap (map name . properties) nvs) nvs

ordNub :: (Ord a) => [a] -> [a]
ordNub = go S.empty
   where
       go _ []     = []
       go s (x:xs) = if x `S.member` s then go s xs
                                     else x : go (S.insert x s) xs

nodesIfAny :: [(NodeID,Node)] -> Result
nodesIfAny [] = Empty
nodesIfAny ns = Nodes ns

edgesIfAny :: [(EdgeID,Edge)] -> Result
edgesIfAny [] = Empty
edgesIfAny es = Edges es
