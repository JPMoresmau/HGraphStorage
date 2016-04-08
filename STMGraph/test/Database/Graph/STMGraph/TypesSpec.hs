{-# LANGUAGE ScopedTypeVariables #-}
-----------------------------------------------------------------------------
--
-- Module      :  Database.Graph.STMGraph.TypesSpec
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

module Database.Graph.STMGraph.TypesSpec where

import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Instances

import Database.Graph.STMGraph.Types

import Data.Default
import qualified Data.Map                               as DM

import Control.Concurrent.Async
import Control.Monad.STM
import qualified STMContainers.Map as SM
import Control.Concurrent.STM.TVar

spec :: Spec
spec =
  describe "Basic operations" $ do
    it "serializes models correctly" $ property $
        \x -> (stringToModel . modelToString) x == x
    it "lookups to names correctly" $ property $
        \(a::String) (b::Int) -> DM.lookup a (toName $ addToLookup a b def) == Just b
    it "lookups from names correctly" $ property $
        \(a::String) (b::Int) -> DM.lookup b (fromName $ addToLookup a b def) == Just a
    it "generates ID correctly" $ do
        ig <- atomically $ newIDGen 1
        i1 <- atomically $ nextID ig 1
        i1 `shouldBe` 1
        i2 <- atomically $ nextID ig 1
        i2 `shouldBe` 2
        atomically $ freeID i2 1 ig
        i3 <- atomically $ nextID ig 1
        i3 `shouldBe` 2
        atomically $ freeID i1 1 ig
        i4 <- atomically $ nextID ig 1
        i4 `shouldBe` 1
        i5 <- atomically $ nextID ig 1
        i5 `shouldBe` 3
        atomically $ freeID i4 1 ig
        atomically $ freeID i3 1 ig
        atomically $ freeID i5 1 ig
        m <- atomically $ readTVar (freeIDs ig)
        DM.null m `shouldBe` True


instance (Ord a, Ord b,Arbitrary a, Arbitrary b)=> Arbitrary (Lookup a b) where
  arbitrary = lookupFromList <$> arbitrary

instance Arbitrary Model where
    arbitrary = Model <$> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary DataType where
    arbitrary = oneof [return DTText, return DTInteger, return DTBinary]
