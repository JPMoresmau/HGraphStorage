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

import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Instances

import Database.Graph.STMGraph.API
import Database.Graph.STMGraph.Constants
import Database.Graph.STMGraph.Raw
import Database.Graph.STMGraph.Types

import Database.Graph.STMGraph.RawSpec

import Control.Monad.STM
import Data.Default
import Data.Monoid

import qualified Data.Text as T

spec :: Spec
spec = do
  describe "Update operations" $ do
    withEmptyDB "creates nodes" $ \db->do
        nnb0 <- atomically $ nbNodes db
        nnb0 `shouldBe` 0
        oid1 <- atomically $ addNode db "type1" [nm "obj0",cnt 1]
        oid1 `shouldBe` 1
        nnb0 <- atomically $ nbNodes db
        nnb0 `shouldBe` 1
        o1 <- atomically $ readNode db oid1
        o1 `shouldBe` Node 1 def def 2
        (p1,pv1) <- atomically $ readProperty db 1
        pv1 `shouldBe` PVText "obj0"
        pNext p1 `shouldBe` def
        (p2,pv2) <- atomically $ readProperty db 2
        pv2 `shouldBe` PVInteger 1
        pNext p2 `shouldBe` 1
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        o2 <- atomically $ readNode db oid2
        o2 `shouldBe` Node 1 def def 4
    withEmptyDB "creates edges" $ \db->do
        nnb0 <- atomically $ nbNodes db
        nnb0 `shouldBe` 0
        enb0 <- atomically $ nbEdges db
        enb0 `shouldBe` 0
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        nnb1 <- atomically $ nbNodes db
        nnb1 `shouldBe` 2
        enb1 <- atomically $ nbEdges db
        enb1 `shouldBe` 1
        eid1  `shouldBe` 1
        e1 <- atomically $ readEdge db eid1
        e1 `shouldBe` Edge oid1 1 oid2 1 1 def def 5
        o1 <- atomically $ readNode db oid1
        o1 `shouldBe` Node 1 eid1 def 2
        o2 <- atomically $ readNode db oid2
        o2 `shouldBe` Node 1 def eid1 4
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        eid2  `shouldBe` 2
        e1' <- atomically $ readEdge db eid1
        e1' `shouldBe` Edge oid1 1 oid2 1 1 def def 5
        e2 <- atomically $ readEdge db eid2
        e2 `shouldBe` Edge oid1 1 oid2 1 2 eid1 eid1 6
        o1' <- atomically $ readNode db oid1
        o1' `shouldBe` Node 1 eid2 def 2
        o2' <- atomically $ readNode db oid2
        o2' `shouldBe` Node 1 def eid2 4
    withEmptyDB "deletes nodes" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref1" [w 1] oid3
        eid3 <- atomically $ addEdge db oid2 "ref1" [w 1] oid3
        nnb0 <- atomically $ nbNodes db
        nnb0 `shouldBe` 3
        enb0 <- atomically $ nbEdges db
        enb0 `shouldBe` 3
        atomically $ removeNode db oid1
        o1 <- atomically $ readNode db oid1
        o1 `shouldBe` def
        o2 <- atomically $ readNode db oid2
        o2 `shouldBe` Node 1 3 0 4
        o3 <- atomically $ readNode db oid3
        o3 `shouldBe` Node 1 0 3 6
        nnb1 <- atomically $ nbNodes db
        nnb1 `shouldBe` 2
        enb1 <- atomically $ nbEdges db
        enb1 `shouldBe` 1
    withEmptyDB "deletes edges" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref1" [w 1] oid3
        eid3 <- atomically $ addEdge db oid2 "ref1" [w 1] oid3
        nnb0 <- atomically $ nbNodes db
        nnb0 `shouldBe` 3
        enb0 <- atomically $ nbEdges db
        enb0 `shouldBe` 3
        e2 <- atomically $ readEdge db eid2
        e2 `shouldBe` Edge oid1 1 oid3 1 1 eid1 def 8
        e3 <- atomically $ readEdge db eid3
        e3 `shouldBe` Edge oid2 1 oid3 1 1 def eid2 9
        atomically $ removeEdge db eid1
        e1 <- atomically $ readEdge db eid1
        e1 `shouldBe` def
        nnb1 <- atomically $ nbNodes db
        nnb1 `shouldBe` 3
        enb1 <- atomically $ nbEdges db
        enb1 `shouldBe` 2
        o1 <- atomically $ readNode db oid1
        o1 `shouldBe` Node 1 2 def 2
        o2 <- atomically $ readNode db oid2
        o2 `shouldBe` Node 1 3 0 4
        o3 <- atomically $ readNode db oid3
        o3 `shouldBe` Node 1 0 3 6
        e2' <- atomically $ readEdge db eid2
        e2' `shouldBe` Edge oid1 1 oid3 1 1 def def 8
        e3' <- atomically $ readEdge db eid3
        e3' `shouldBe` Edge oid2 1 oid3 1 1 def eid2 9
        atomically $ removeEdge db eid2
        nnb2 <- atomically $ nbNodes db
        nnb2 `shouldBe` 3
        enb2 <- atomically $ nbEdges db
        enb2 `shouldBe` 1
        o1' <- atomically $ readNode db oid1
        o1' `shouldBe` Node 1 def def 2
        e3'' <- atomically $ readEdge db eid3
        e3'' `shouldBe` Edge oid2 1 oid3 1 1 def def 9
    withEmptyDB "updates properties" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj0",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 2]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        p1 <- atomically $ nodeProperties db oid1 return
        p1 `shouldBe` [nm "obj0",cnt 1]
        p2 <- atomically $ nodeProperties db oid1 (\ps->return ((s 3) : ps))
        p2 `shouldBe` [s 3,nm "obj0",cnt 1]
        p3 <- atomically $ nodeProperties db oid1 return
        p3 `shouldBe` p2
        p4 <- atomically $ edgeProperties db eid1 return
        p4 `shouldBe` [w 1]
        p5 <- atomically $ edgeProperties db eid1 (\ps->return $ (s 2) : ps)
        p5 `shouldBe` [s 2,w 1]
        p6 <- atomically $ edgeProperties db eid1 return
        p6 `shouldBe` p5
  describe "Node Traversals" $ do
    withEmptyDB "finds node by id" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj0",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> NID  [1])
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
    withEmptyDB "doesn't find unexisting node by id" $ \db->do
        r <- atomically $ traverseGraph db (Ns <> NID  [1])
        r `shouldBe` Empty
    withEmptyDB "finds node by id and value" $ \db-> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> NID  [1,2] <> Has (nm "obj1"))
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
    withEmptyDB "finds node by value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> Has (nm "obj1"))
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
    withEmptyDB "doesn't find node by id and unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> NID  [1,2] <> Has (nm "obj3"))
        r `shouldBe` Empty
    withEmptyDB "doesn't find node by unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> Has (nm "obj3"))
        r `shouldBe` Empty
  describe "Edge Traversals" $ do
    withEmptyDB "finds edge by id" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj0",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1])
        r `shouldBe` Edges [(eid1,Edge oid1 1 oid2 1 1 def def 5)]
    withEmptyDB "doesn't find unexisting edge by id" $ \db->do
        r <- atomically $ traverseGraph db (Es <> EID  [1])
        r `shouldBe` Empty
    withEmptyDB "finds edge by id and value" $ \db-> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1,2] <> Has (w 1))
        r `shouldBe` Edges [(oid1,Edge oid1 1 oid2 1 1 def def 5)]
    withEmptyDB "finds edge by value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> Has (w 1))
        r `shouldBe` Edges [(oid1,Edge oid1 1 oid2 1 1 def def 5)]
    withEmptyDB "doesn't find edge by id and unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1,2] <> Has (w 3))
        r `shouldBe` Empty
    withEmptyDB "doesn't find edge by unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> Has (w 3))
        r `shouldBe` Empty
  describe "Returning values" $ do
    withEmptyDB "get specific values for nodes" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1, w 2]
        r <- atomically $ traverseGraph db (Ns <> NID  [1,2] <> Values ["nm","weight"])
        r `shouldBe` Properties ["nm","weight"] [NodeInfo 1 "type1" [nm "obj1"],NodeInfo 2 "type1" [nm "obj2",w 2]]
    withEmptyDB "unknown properties for nodes" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1, w 2]
        r <- atomically $ traverseGraph db (Ns <> NID  [1,2] <> Values ["nm2","weight2"])
        r `shouldBe` Properties ["nm2","weight2"] [NodeInfo 1 "type1" [],NodeInfo 2 "type1" []]
    withEmptyDB "get all values for nodes" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1, w 2]
        r <- atomically $ traverseGraph db (Ns <> NID  [1,2] <> AllValues)
        r `shouldBe` Properties ["nm","cnt","weight"] [NodeInfo 1 "type1" [nm "obj1",cnt 1],NodeInfo 2 "type1" [nm "obj2",cnt 1,w 2]]
    withEmptyDB "get specific values for all nodes" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1, w 2]
        r <- atomically $ traverseGraph db (Ns <> Values ["nm","weight"])
        r `shouldBe` Properties ["nm","weight"] [NodeInfo 2 "type1" [nm "obj2",w 2],NodeInfo 1 "type1" [nm "obj1"]] -- order reversed
    withEmptyDB "unknown properties for all nodes" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1, w 2]
        r <- atomically $ traverseGraph db (Ns <> Values ["nm2","weight2"])
        r `shouldBe` Properties ["nm2","weight2"] [NodeInfo 2 "type1" [],NodeInfo 1 "type1"[]]
    withEmptyDB "get all values for all nodes" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1, w 2]
        r <- atomically $ traverseGraph db (Ns <> AllValues)
        r `shouldBe` Properties ["nm","cnt","weight"] [NodeInfo 2 "type1" [nm "obj2",cnt 1,w 2],NodeInfo 1 "type1" [nm "obj1",cnt 1]] -- order reversed
    withEmptyDB "get specific values for edges" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1,2] <> Values ["since","weight"])
        r `shouldBe` Properties ["since","weight"] [EdgeInfo 1 "ref1" [w 1],EdgeInfo 2 "ref2" [w 2,s 0]]
    withEmptyDB "unknown properties for edges" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1,2] <> Values ["since2","weight2"])
        r `shouldBe` Properties ["since2","weight2"] [EdgeInfo 1 "ref1" [],EdgeInfo 2 "ref2" []]
    withEmptyDB "get all values for edges" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1,2] <> AllValues)
        r `shouldBe` Properties ["weight","since"] [EdgeInfo 1 "ref1" [w 1],EdgeInfo 2 "ref2" [w 2,s 0]]
    withEmptyDB "get specific values for all edges" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid2
        r <- atomically $ traverseGraph db (Es <> Values ["since","weight"])
        r `shouldBe` Properties ["since","weight"] [EdgeInfo 2 "ref2" [w 2,s 0],EdgeInfo 1 "ref1" [w 1]] -- order reversed
    withEmptyDB "unknown properties for all edges" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid2
        r <- atomically $ traverseGraph db (Es <> Values ["since2","weight2"])
        r `shouldBe` Properties ["since2","weight2"] [EdgeInfo 2 "ref2" [],EdgeInfo 1 "ref1" []]
    withEmptyDB "get all values for all edges" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid2
        r <- atomically $ traverseGraph db (Es <> AllValues)
        r `shouldBe` Properties ["weight","since"] [EdgeInfo 2 "ref2" [w 2,s 0],EdgeInfo 1 "ref1" [w 1]] -- order reversed
  describe "Out edges" $ do
     withEmptyDB "specific edge types" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid3
        r0 <- atomically $ traverseGraph db (Ns <> NID  [1] <> Out ["ref1"])
        r0 `shouldBe` Nodes [(oid2,Node 1 def 1 4)]
        r1 <- atomically $ traverseGraph db (Ns <> NID  [1] <> Out ["ref1","ref2"])
        r1 `shouldBe` Nodes [(oid2,Node 1 def 1 4),(oid3,Node 1 def 2 6)]
        r2 <- atomically $ traverseGraph db (Ns <> Out ["ref1","ref2"])
        r2 `shouldBe` Nodes [(oid2,Node 1 def 1 4),(oid3,Node 1 def 2 6)]
     withEmptyDB "all edge types" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid3
        r0 <- atomically $ traverseGraph db (Ns <> NID  [1] <> Out ["*"])
        r0 `shouldBe` Nodes [(oid2,Node 1 def 1 4),(oid3,Node 1 def 2 6)]
     withEmptyDB "unknown edge types" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid3
        r0 <- atomically $ traverseGraph db (Ns <> NID  [1] <> Out ["ref3","ref4"])
        r0 `shouldBe` Empty
        r1 <- atomically $ traverseGraph db (Ns <> NID  [1] <> Out ["ref1","ref3"])
        r1 `shouldBe` Nodes [(oid2,Node 1 def 1 4)]
  describe "In edges" $ do
     withEmptyDB "specific edge types" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid2 "ref1" [w 1] oid1
        eid2 <- atomically $ addEdge db oid3 "ref2" [w 2,s 0] oid1
        r0 <- atomically $ traverseGraph db (Ns <> NID  [1] <> In ["ref1"])
        r0 `shouldBe` Nodes [(oid2,Node 1 1 def 4)]
        r1 <- atomically $ traverseGraph db (Ns <> NID  [1] <> In ["ref1","ref2"])
        r1 `shouldBe` Nodes [(oid2,Node 1 1 def 4),(oid3,Node 1 2 def 6)]
        r2 <- atomically $ traverseGraph db (Ns <> In ["ref1","ref2"])
        r2 `shouldBe` Nodes [(oid2,Node 1 1 def 4),(oid3,Node 1 2 def 6)]
     withEmptyDB "all edge types" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid2 "ref1" [w 1] oid1
        eid2 <- atomically $ addEdge db oid3 "ref2" [w 2,s 0] oid1
        r0 <- atomically $ traverseGraph db (Ns <> NID  [1] <> In ["*"])
        r0 `shouldBe` Nodes [(oid2,Node 1 1 def 4),(oid3,Node 1 2 def 6)]
     withEmptyDB "unknown edge types" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid2 "ref1" [w 1] oid1
        eid2 <- atomically $ addEdge db oid3 "ref2" [w 2,s 0] oid1
        r0 <- atomically $ traverseGraph db (Ns <> NID  [1] <> In ["ref3","ref4"])
        r0 `shouldBe` Empty
        r1 <- atomically $ traverseGraph db (Ns <> NID  [1] <> In ["ref1","ref3"])
        r1 `shouldBe` Nodes [(oid2,Node 1 1 def 4)]
  describe "Both edges" $ do
     withEmptyDB "specific edge types" $ \db -> do
        oid1 <- atomically $ addNode db "type1" [nm "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [nm "obj2",cnt 1]
        oid3 <- atomically $ addNode db "type1" [nm "obj3",cnt 1]
        eid1 <- atomically $ addEdge db oid2 "ref1" [w 1] oid1
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2,s 0] oid3
        r0 <- atomically $ traverseGraph db (Ns <> NID  [1] <> Both ["ref1"])
        r0 `shouldBe` Nodes [(oid2,Node 1 1 def 4)]
        r1 <- atomically $ traverseGraph db (Ns <> NID  [1] <> Both ["ref1","ref2"])
        r1 `shouldBe` Nodes [(oid2,Node 1 1 def 4),(oid3,Node 1 def 2 6)]
        r2 <- atomically $ traverseGraph db (Ns <> Both ["ref1","ref2"])
        r2 `shouldBe` Nodes [(oid1,Node 1 2 1 2),(oid2,Node 1 1 def 4),(oid1,Node 1 2 1 2),(oid3,Node 1 def 2 6)]

nm :: T.Text -> NameValue
nm = TextP "nm"

cnt :: Integer -> NameValue
cnt = IntP "cnt"

w :: Integer -> NameValue
w = IntP "weight"

s :: Integer -> NameValue
s = IntP "since"
