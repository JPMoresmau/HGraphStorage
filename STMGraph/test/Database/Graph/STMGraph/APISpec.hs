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
        oid1 <- atomically $ addNode db "type1" [name "obj0",cnt 1]
        oid1 `shouldBe` 1
        o1 <- atomically $ readNode db oid1
        o1 `shouldBe` Node 1 def def 2
        (p1,pv1) <- atomically $ readProperty db 1
        pv1 `shouldBe` PVText "obj0"
        pNext p1 `shouldBe` def
        (p2,pv2) <- atomically $ readProperty db 2
        pv2 `shouldBe` PVInteger 1
        pNext p2 `shouldBe` 1
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        o2 <- atomically $ readNode db oid2
        o2 `shouldBe` Node 1 def def 4
    withEmptyDB "creates edges" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
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
  describe "Node Traversals" $ do
    withEmptyDB "finds node by id" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj0",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> NID  [1])
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
    withEmptyDB "doesn't find unexisting node by id" $ \db->do
        r <- atomically $ traverseGraph db (Ns <> NID  [1])
        r `shouldBe` Empty
    withEmptyDB "finds node by id and value" $ \db-> do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> NID  [1,2] <> Has (name "obj1"))
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
    withEmptyDB "finds node by value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> Has (name "obj1"))
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
    withEmptyDB "doesn't find node by id and unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> NID  [1,2] <> Has (name "obj3"))
        r `shouldBe` Empty
    withEmptyDB "doesn't find node by unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db (Ns <> Has (name "obj3"))
        r `shouldBe` Empty
  describe "Edge Traversals" $ do
    withEmptyDB "finds edge by id" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj0",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1])
        r `shouldBe` Edges [(eid1,Edge oid1 1 oid2 1 1 def def 5)]
    withEmptyDB "doesn't find unexisting edge by id" $ \db->do
        r <- atomically $ traverseGraph db (Es <> EID  [1])
        r `shouldBe` Empty
    withEmptyDB "finds edge by id and value" $ \db-> do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1,2] <> Has (w 1))
        r `shouldBe` Edges [(oid1,Edge oid1 1 oid2 1 1 def def 5)]
    withEmptyDB "finds edge by value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> Has (w 1))
        r `shouldBe` Edges [(oid1,Edge oid1 1 oid2 1 1 def def 5)]
    withEmptyDB "doesn't find edge by id and unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> EID  [1,2] <> Has (w 3))
        r `shouldBe` Empty
    withEmptyDB "doesn't find edge by unknown value" $ \db->do
        oid1 <- atomically $ addNode db "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db "type1" [name "obj2",cnt 1]
        eid1 <- atomically $ addEdge db oid1 "ref1" [w 1] oid2
        eid2 <- atomically $ addEdge db oid1 "ref2" [w 2] oid2
        r <- atomically $ traverseGraph db (Es <> Has (w 3))
        r `shouldBe` Empty

name :: T.Text -> NameValue
name = TextP "name"

cnt :: Integer -> NameValue
cnt = IntP "cnt"

w :: Integer -> NameValue
w = IntP "weight"
