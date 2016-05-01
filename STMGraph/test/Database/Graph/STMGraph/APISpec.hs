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
    it "creates node" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        oid1 <- atomically $ addNode db0 "type1" [name "obj0",cnt 1]
        oid1 `shouldBe` 1
        o1 <- atomically $ readNode db0 oid1
        o1 `shouldBe` (Node 1 def def 2)
        (p1,pv1) <- atomically $ readProperty db0 1
        pv1 `shouldBe` (PVText "obj0")
        pNext p1 `shouldBe` def
        (p2,pv2) <- atomically $ readProperty db0 2
        pv2 `shouldBe` (PVInteger 1)
        pNext p2 `shouldBe` 1
        close db0
  describe "Traversals" $ do
    it "finds node by id" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        oid1 <- atomically $ addNode db0 "type1" [name "obj0",cnt 1]
        r <- atomically $ traverseGraph db0 (Ns <> NID  [1])
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
        close db0
    it "doesn't find unexisting node by id" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        r <- atomically $ traverseGraph db0 (Ns <> NID  [1])
        r `shouldBe` Empty
        close db0
    it "finds node by id and value" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        oid1 <- atomically $ addNode db0 "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db0 "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db0 (Ns <> NID  [1,2] <> Has (name "obj1"))
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
        close db0
    it "finds node by value" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        oid1 <- atomically $ addNode db0 "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db0 "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db0 (Ns <> Has (name "obj1"))
        r `shouldBe` Nodes [(oid1,Node 1 def def 2)]
        close db0
    it "doesn't find node by id and unknown value" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        oid1 <- atomically $ addNode db0 "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db0 "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db0 (Ns <> NID  [1,2] <> Has (name "obj3"))
        r `shouldBe` Empty
        close db0
    it "doesn't find node by unknown value" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        oid1 <- atomically $ addNode db0 "type1" [name "obj1",cnt 1]
        oid2 <- atomically $ addNode db0 "type1" [name "obj2",cnt 1]
        r <- atomically $ traverseGraph db0 (Ns <> Has (name "obj3"))
        r `shouldBe` Empty
        close db0

name :: T.Text -> NameValue
name = TextP "name"

cnt :: Integer -> NameValue
cnt = IntP "cnt"
