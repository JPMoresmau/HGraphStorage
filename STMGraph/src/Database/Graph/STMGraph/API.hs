{-# LANGUAGE DeriveDataTypeable, OverloadedStrings #-}
module Database.Graph.STMGraph.API
  ( addNode
  , addEdge
  , NameValue(..)
  , traverseGraph
  , Traversal(..)
  , Result(..)
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

createProperties :: Database -> [NameValue] -> STM PropertyID
createProperties db = foldM createProperty def . map toPropertyValue
  where
    createProperty nid (name,value) = do
      let dt=valueType value
      ptid <- getPropertyTypeID db (name,dt)
      writeProperty db Nothing ((ptid,nid),value)

getNodeProperties :: Database -> Node -> STM [NameValue]
getNodeProperties db n = getProperties db $ nFirstProperty n

getEdgeProperties :: Database -> Edge -> STM [NameValue]
getEdgeProperties db e = getProperties db $ eFirstProperty e

getProperties :: Database -> PropertyID -> STM [NameValue]
getProperties db fp = readProp fp []
    where
      readProp fid ls
        | fid == def = return ls
        | otherwise = do
            (p,v) <- readProperty db fid
            mn <- getPropertyType db (pType p)
            case mn of
                Nothing -> error $ "unknown property type:" <> show (pType p)
                Just n -> readProp (pNext p) (toNameValue (fst n,v):ls)

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

doTraverse :: Database -> Result -> Traversal ->STM Result
doTraverse db Empty _ = return Empty
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
doTraverse _ r t = return $ Error $ "Traversal not handled: " <> T.pack (show t) <> " in state: " <> T.pack (show r)

nodesIfAny :: [(NodeID,Node)] -> Result
nodesIfAny [] = Empty
nodesIfAny ns = Nodes ns

edgesIfAny :: [(EdgeID,Edge)] -> Result
edgesIfAny [] = Empty
edgesIfAny es = Edges es
