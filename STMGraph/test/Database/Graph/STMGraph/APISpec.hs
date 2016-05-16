{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
-----------------------------------------------------------------------------
--
-- Module      :  Database.Graph.STMGraph.APISpec
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

module Database.Graph.STMGraph.APISpec where

import Test.Hspec hiding (shouldBe)
import qualified Test.Hspec as HS (shouldBe)
import Test.QuickCheck
import Test.QuickCheck.Instances

import Database.Graph.STMGraph.API
import Database.Graph.STMGraph.Constants
import Database.Graph.STMGraph.Raw
import Database.Graph.STMGraph.Types

import Database.Graph.STMGraph.RawSpec
import Control.Monad.IO.Class ( liftIO)
import Control.Monad.STM
import Data.Default
import Data.Monoid

import qualified Data.Text as T

spec :: Spec
spec = do
  describe "Update operations" $ do
    withEmptyDB' "creates nodes" $ do
        nnb0 <- nbNodes
        nnb0 `shouldBe` 0
        oid1 <- addNode "type1" [nm "obj0",cnt 1]
        oid1 `shouldBe` 1
        nnb0 <- nbNodes
        nnb0 `shouldBe` 1
        db <- getDatabase
        o1 <- liftIO $ atomically $ readNode db oid1
        o1 `shouldBe` Node 1 def def 2
        (p1,pv1) <- liftIO $ atomically $ readProperty db 1
        pv1 `shouldBe` PVText "obj0"
        pNext p1 `shouldBe` def
        (p2,pv2) <- liftIO $ atomically $ readProperty db 2
        pv2 `shouldBe` PVInteger 1
        pNext p2 `shouldBe` 1
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        o2 <- liftIO $ atomically $ readNode db oid2
        o2 `shouldBe` Node 1 def def 4
    withEmptyDB' "creates edges" $ do
        nnb0 <- nbNodes
        nnb0 `shouldBe` 0
        enb0 <- nbEdges
        enb0 `shouldBe` 0
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        nnb1 <- nbNodes
        nnb1 `shouldBe` 2
        enb1 <- nbEdges
        enb1 `shouldBe` 1
        eid1  `shouldBe` 1
        db <- getDatabase
        e1 <- liftIO $ atomically $ readEdge db eid1
        e1 `shouldBe` Edge oid1 1 oid2 1 1 def def 5
        o1 <- liftIO $ atomically $ readNode db oid1
        o1 `shouldBe` Node 1 eid1 def 2
        o2 <- liftIO $ atomically $ readNode db oid2
        o2 `shouldBe` Node 1 def eid1 4
        eid2 <- addEdge oid1 "ref2" [w 2] oid2
        eid2  `shouldBe` 2
        e1' <- liftIO $ atomically $ readEdge db eid1
        e1' `shouldBe` Edge oid1 1 oid2 1 1 def def 5
        e2 <- liftIO $ atomically $ readEdge db eid2
        e2 `shouldBe` Edge oid1 1 oid2 1 2 eid1 eid1 6
        o1' <- liftIO $ atomically $ readNode db oid1
        o1' `shouldBe` Node 1 eid2 def 2
        o2' <- liftIO $ atomically $ readNode db oid2
        o2' `shouldBe` Node 1 def eid2 4
    withEmptyDB' "deletes nodes" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref1" [w 1] oid3
        eid3 <- addEdge oid2 "ref1" [w 1] oid3
        nnb0 <- nbNodes
        nnb0 `shouldBe` 3
        enb0 <- nbEdges
        enb0 `shouldBe` 3
        removeNode oid1
        db <- getDatabase
        o1 <- liftIO $ atomically $ readNode db oid1
        o1 `shouldBe` def
        o2 <- liftIO $ atomically $ readNode db oid2
        o2 `shouldBe` Node 1 3 0 4
        o3 <- liftIO $ atomically $ readNode db oid3
        o3 `shouldBe` Node 1 0 3 6
        nnb1 <- nbNodes
        nnb1 `shouldBe` 2
        enb1 <- nbEdges
        enb1 `shouldBe` 1
    withEmptyDB' "deletes edges" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref1" [w 1] oid3
        eid3 <- addEdge oid2 "ref1" [w 1] oid3
        nnb0 <- nbNodes
        nnb0 `shouldBe` 3
        enb0 <- nbEdges
        enb0 `shouldBe` 3
        db <- getDatabase
        e2 <- liftIO $ atomically $ readEdge db eid2
        e2 `shouldBe` Edge oid1 1 oid3 1 1 eid1 def 8
        e3 <- liftIO $ atomically $ readEdge db eid3
        e3 `shouldBe` Edge oid2 1 oid3 1 1 def eid2 9
        removeEdge eid1
        e1 <- liftIO $ atomically $ readEdge db eid1
        e1 `shouldBe` def
        nnb1 <- nbNodes
        nnb1 `shouldBe` 3
        enb1 <- nbEdges
        enb1 `shouldBe` 2
        o1 <- liftIO $ atomically $ readNode db oid1
        o1 `shouldBe` Node 1 2 def 2
        o2 <- liftIO $ atomically $ readNode db oid2
        o2 `shouldBe` Node 1 3 0 4
        o3 <- liftIO $ atomically $ readNode db oid3
        o3 `shouldBe` Node 1 0 3 6
        e2' <- liftIO $ atomically $ readEdge db eid2
        e2' `shouldBe` Edge oid1 1 oid3 1 1 def def 8
        e3' <- liftIO $ atomically $ readEdge db eid3
        e3' `shouldBe` Edge oid2 1 oid3 1 1 def eid2 9
        removeEdge eid2
        nnb2 <- nbNodes
        nnb2 `shouldBe` 3
        enb2 <- nbEdges
        enb2 `shouldBe` 1
        o1' <- liftIO $ atomically $ readNode db oid1
        o1' `shouldBe` Node 1 def def 2
        e3'' <- liftIO $ atomically $ readEdge db eid3
        e3'' `shouldBe` Edge oid2 1 oid3 1 1 def def 9
    withEmptyDB' "updates properties" $ do
        oid1 <- addNode "type1" [nm "obj0",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 2]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        p1 <- nodeProperties oid1 return
        p1 `shouldBe` [nm "obj0",cnt 1]
        p2 <- nodeProperties oid1 (\ps->return ((s 3) : ps))
        p2 `shouldBe` [s 3,nm "obj0",cnt 1]
        p3 <- nodeProperties oid1 return
        p3 `shouldBe` p2
        p4 <- edgeProperties eid1 return
        p4 `shouldBe` [w 1]
        p5 <- edgeProperties eid1 (\ps->return $ (s 2) : ps)
        p5 `shouldBe` [s 2,w 1]
        p6 <- edgeProperties eid1 return
        p6 `shouldBe` p5
  describe "Node Traversals" $ do
    withEmptyDB' "finds node by id" $ do
        oid1 <- addNode "type1" [nm "obj0",cnt 1]
        r <- traverseGraph (Ns <> NID  [1])
        r `shouldBe` Nodes [oid1]
    withEmptyDB' "doesn't find unexisting node by id" $ do
        r <- traverseGraph (Ns <> NID  [1])
        r `shouldBe` Empty
    withEmptyDB' "finds node by id and value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        r <- traverseGraph (Ns <> NID  [1,2] <> Has (nm "obj1"))
        r `shouldBe` Nodes [oid1]
    withEmptyDB' "finds node by value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        r <- traverseGraph (Ns <> Has (nm "obj1"))
        r `shouldBe` Nodes [oid1]
    withEmptyDB' "doesn't find node by id and unknown value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        r <- traverseGraph (Ns <> NID  [1,2] <> Has (nm "obj3"))
        r `shouldBe` Empty
    withEmptyDB' "doesn't find node by unknown value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        r <- traverseGraph (Ns <> Has (nm "obj3"))
        r `shouldBe` Empty
  describe "Edge Traversals" $ do
    withEmptyDB' "finds edge by id" $ do
        oid1 <- addNode "type1" [nm "obj0",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        r <- traverseGraph (Es <> EID  [1])
        r `shouldBe` Edges [eid1]
    withEmptyDB' "doesn't find unexisting edge by id" $do
        r <- traverseGraph (Es <> EID  [1])
        r `shouldBe` Empty
    withEmptyDB' "finds edge by id and value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2] oid2
        r <- traverseGraph (Es <> EID  [1,2] <> Has (w 1))
        r `shouldBe` Edges [oid1]
    withEmptyDB' "finds edge by value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2] oid2
        r <- traverseGraph (Es <> Has (w 1))
        r `shouldBe` Edges [oid1]
    withEmptyDB' "doesn't find edge by id and unknown value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2] oid2
        r <- traverseGraph (Es <> EID  [1,2] <> Has (w 3))
        r `shouldBe` Empty
    withEmptyDB' "doesn't find edge by unknown value" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2] oid2
        r <- traverseGraph (Es <> Has (w 3))
        r `shouldBe` Empty
  describe "Returning values" $ do
    withEmptyDB' "get specific values for nodes" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1, w 2]
        r <- traverseGraph (Ns <> NID  [1,2] <> Values ["nm","weight"])
        r `shouldBe` Properties ["nm","weight"] [NodeInfo 1 "type1" [nm "obj1"],NodeInfo 2 "type1" [nm "obj2",w 2]]
    withEmptyDB' "unknown properties for nodes" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1, w 2]
        r <- traverseGraph (Ns <> NID  [1,2] <> Values ["nm2","weight2"])
        r `shouldBe` Properties ["nm2","weight2"] [NodeInfo 1 "type1" [],NodeInfo 2 "type1" []]
    withEmptyDB' "get all values for nodes" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1, w 2]
        r <- traverseGraph (Ns <> NID  [1,2] <> AllValues)
        r `shouldBe` Properties ["nm","cnt","weight"] [NodeInfo 1 "type1" [nm "obj1",cnt 1],NodeInfo 2 "type1" [nm "obj2",cnt 1,w 2]]
    withEmptyDB' "get specific values for all nodes" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1, w 2]
        r <- traverseGraph (Ns <> Values ["nm","weight"])
        r `shouldBe` Properties ["nm","weight"] [NodeInfo 2 "type1" [nm "obj2",w 2],NodeInfo 1 "type1" [nm "obj1"]] -- order reversed
    withEmptyDB' "unknown properties for all nodes" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1, w 2]
        r <- traverseGraph (Ns <> Values ["nm2","weight2"])
        r `shouldBe` Properties ["nm2","weight2"] [NodeInfo 2 "type1" [],NodeInfo 1 "type1"[]]
    withEmptyDB' "get all values for all nodes" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1, w 2]
        r <- traverseGraph (Ns <> AllValues)
        r `shouldBe` Properties ["nm","cnt","weight"] [NodeInfo 2 "type1" [nm "obj2",cnt 1,w 2],NodeInfo 1 "type1" [nm "obj1",cnt 1]] -- order reversed
    withEmptyDB' "get specific values for edges" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid2
        r <- traverseGraph (Es <> EID  [1,2] <> Values ["since","weight"])
        r `shouldBe` Properties ["since","weight"] [EdgeInfo 1 "ref1" [w 1],EdgeInfo 2 "ref2" [w 2,s 0]]
    withEmptyDB' "unknown properties for edges" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid2
        r <- traverseGraph (Es <> EID  [1,2] <> Values ["since2","weight2"])
        r `shouldBe` Properties ["since2","weight2"] [EdgeInfo 1 "ref1" [],EdgeInfo 2 "ref2" []]
    withEmptyDB' "get all values for edges" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid2
        r <- traverseGraph (Es <> EID  [1,2] <> AllValues)
        r `shouldBe` Properties ["weight","since"] [EdgeInfo 1 "ref1" [w 1],EdgeInfo 2 "ref2" [w 2,s 0]]
    withEmptyDB' "get specific values for all edges" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid2
        r <- traverseGraph (Es <> Values ["since","weight"])
        r `shouldBe` Properties ["since","weight"] [EdgeInfo 2 "ref2" [w 2,s 0],EdgeInfo 1 "ref1" [w 1]] -- order reversed
    withEmptyDB' "unknown properties for all edges" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid2
        r <- traverseGraph (Es <> Values ["since2","weight2"])
        r `shouldBe` Properties ["since2","weight2"] [EdgeInfo 2 "ref2" [],EdgeInfo 1 "ref1" []]
    withEmptyDB' "get all values for all edges" $ do
        oid1 <- addNode  "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid2
        r <- traverseGraph (Es <> AllValues)
        r `shouldBe` Properties ["weight","since"] [EdgeInfo 2 "ref2" [w 2,s 0],EdgeInfo 1 "ref1" [w 1]] -- order reversed
  describe "Out edges" $ do
     withEmptyDB' "specific edge types" $ do
        oid1 <- addNode  "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge  oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid3
        r0 <- traverseGraph (Ns <> NID  [1] <> Out ["ref1"])
        r0 `shouldBe` Nodes [oid2]
        r1 <- traverseGraph (Ns <> NID  [1] <> Out ["ref1","ref2"])
        r1 `shouldBe` Nodes [oid2,oid3]
        r2 <- traverseGraph (Ns <> Out ["ref1","ref2"])
        r2 `shouldBe` Nodes [oid2,oid3]
        r3 <- traverseGraph  (Ns <> OutE ["ref1","ref2"])
        r3 `shouldBe` Edges [eid1,oid2]
     withEmptyDB' "all edge types" $ do
        oid1 <- addNode  "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid3
        r0 <- traverseGraph (Ns <> NID  [1] <> Out ["*"])
        r0 `shouldBe` Nodes [oid2,oid3]
     withEmptyDB' "unknown edge types" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid1 "ref1" [w 1] oid2
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid3
        r0 <- traverseGraph (Ns <> NID  [1] <> Out ["ref3","ref4"])
        r0 `shouldBe` Empty
        r1 <- traverseGraph (Ns <> NID  [1] <> Out ["ref1","ref3"])
        r1 `shouldBe` Nodes [oid2]
  describe "In edges" $ do
     withEmptyDB' "specific edge types" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid2 "ref1" [w 1] oid1
        eid2 <- addEdge oid3 "ref2" [w 2,s 0] oid1
        r0 <- traverseGraph (Ns <> NID  [1] <> In ["ref1"])
        r0 `shouldBe` Nodes [oid2]
        r1 <- traverseGraph (Ns <> NID  [1] <> In ["ref1","ref2"])
        r1 `shouldBe` Nodes [oid2,oid3]
        r2 <- traverseGraph (Ns <> In ["ref1","ref2"])
        r2 `shouldBe` Nodes [oid2,oid3]
        r3 <- traverseGraph (Ns <> InE ["ref1","ref2"])
        r3 `shouldBe` Edges [eid1,eid2]
     withEmptyDB' "all edge types" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid2 "ref1" [w 1] oid1
        eid2 <- addEdge oid3 "ref2" [w 2,s 0] oid1
        r0 <- traverseGraph (Ns <> NID  [1] <> In ["*"])
        r0 `shouldBe` Nodes [oid2,oid3]
     withEmptyDB' "unknown edge types" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid2 "ref1" [w 1] oid1
        eid2 <- addEdge oid3 "ref2" [w 2,s 0] oid1
        r0 <- traverseGraph (Ns <> NID  [1] <> In ["ref3","ref4"])
        r0 `shouldBe` Empty
        r1 <- traverseGraph (Ns <> NID  [1] <> In ["ref1","ref3"])
        r1 `shouldBe` Nodes [oid2]
  describe "Both edges" $ do
     withEmptyDB' "specific edge types" $ do
        oid1 <- addNode "type1" [nm "obj1",cnt 1]
        oid2 <- addNode "type1" [nm "obj2",cnt 1]
        oid3 <- addNode "type1" [nm "obj3",cnt 1]
        eid1 <- addEdge oid2 "ref1" [w 1] oid1
        eid2 <- addEdge oid1 "ref2" [w 2,s 0] oid3
        r0 <- traverseGraph (Ns <> NID  [1] <> Both ["ref1"])
        r0 `shouldBe` Nodes [oid2]
        r1 <- traverseGraph (Ns <> NID  [1] <> Both ["ref1","ref2"])
        r1 `shouldBe` Nodes [oid2,oid3]
        r2 <- traverseGraph (Ns <> Both ["ref1","ref2"])
        r2 `shouldBe` Nodes [oid1,oid2,oid1,oid3]
        r3 <- traverseGraph (Ns <> BothE ["ref1","ref2"])
        r3 `shouldBe` Edges [eid2,eid1,eid1,eid2]

nm :: T.Text -> NameValue
nm = TextP "nm"

cnt :: Integer -> NameValue
cnt = IntP "cnt"

w :: Integer -> NameValue
w = IntP "weight"

s :: Integer -> NameValue
s = IntP "since"

--withEmptyDB' :: FilePath -> GraphSettings -> STMGraphT IO a -> IO a
withEmptyDB' nm f =
    it nm $ do
        dir <- testEmptyDir
        withDatabaseIO dir def f

shouldBe a b= liftIO $ a `HS.shouldBe` b
