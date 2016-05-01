{-# LANGUAGE DeriveDataTypeable, OverloadedStrings #-}
module Database.Graph.STMGraph.API
  ( addNode
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
toNameValue (n,PVText v) = (TextP n v)
toNameValue (n,PVInteger v) = (IntP n v)
toNameValue (n,PVBinary v) = (BinP n v)
toNameValue (n,PVJSON v) = (JsonP n v)

addNode :: Database -> T.Text -> [NameValue] -> STM NodeID
addNode db tp props = do
  pid <- createProperties db props
  tid <- getNodeTypeID db tp
  let obj=Node tid def def pid
  writeNode db Nothing obj

createProperties :: Database -> [NameValue] -> STM PropertyID
createProperties db = foldM createProperty def . (map toPropertyValue)
  where
    createProperty nid (name,value) = do
      let dt=valueType value
      ptid <- getPropertyTypeID db (name,dt)
      writeProperty db Nothing ((ptid,nid),value)

getNodeProperties :: Database -> Node -> STM [NameValue]
getNodeProperties db n = do
    let fp = oFirstProperty n
    readProp fp []
    where
      readProp fid ls
        | fid == def = return ls
        | otherwise = do
            (p,v) <- readProperty db fid
            mn <- getPropertyType db (pType p)
            case mn of
                Nothing -> error $ "unknown property type:" <> show (pType p)
                Just n -> readProp (pNext p) ((toNameValue (fst n,v)):ls)

hasNamedValue :: Database -> Node -> NameValue -> STM Bool
hasNamedValue db n nv = do
  nvs <- getNodeProperties db n
  return $ nv `elem` nvs

traverseGraph :: Database -> Traversal -> STM Result
traverseGraph db t = doTraverse db Unknown t

doTraverse :: Database   ->  Result -> Traversal ->STM Result
doTraverse db Empty _ = return Empty
doTraverse db st (Composed ts) = foldM (doTraverse db) st ts
doTraverse db _ Ns = return AllNodes
doTraverse db _ Es = return AllEdges
doTraverse db AllNodes (NID nids) = do
    ns <- filter (\(_,n)-> n/= def) <$> mapM (\nid ->
        do
            n<-readNode db nid
            return (nid,n)) nids
    return $ if null ns then Empty else Nodes ns
doTraverse db (Nodes ns) (NID nids) =  do
    let fs = filter (\(nid,_)->nid `elem` nids) ns
    return $ nodesIfAny fs
doTraverse db _ (NID _) = return Empty
doTraverse db AllNodes (Has nv) = do
  let st = SM.stream $ gdNodes $ dData db
  fs <- ListT.fold (\l (nid,n)->do
    has <- hasNamedValue db n nv
    return $ if has then (nid,n):l else l) [] st
  return $ nodesIfAny fs
doTraverse db (Nodes ns) (Has nv) = do
    fs <- filterM (\(_,n)->hasNamedValue db n nv) ns
    return $ nodesIfAny fs
doTraverse _ r t = return $ Error $ "Traversal not handled: " <> (T.pack $ show t) <> " in state: " <> (T.pack $ show r)

nodesIfAny :: [(NodeID,Node)] -> Result
nodesIfAny [] = Empty
nodesIfAny ns = Nodes ns
