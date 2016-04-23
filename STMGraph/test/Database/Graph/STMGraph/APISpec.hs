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

import qualified Data.Text as T

spec :: Spec
spec = do
  describe "Update operations" $ do
    it "creates node" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        oid1 <- atomically $ addNode db0 "type1" [TextP "name" "obj0",IntP "cnt" 1]
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
