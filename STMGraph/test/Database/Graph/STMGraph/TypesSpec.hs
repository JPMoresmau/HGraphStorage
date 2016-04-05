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

spec :: Spec
spec =
  describe "Basic operations" $ do
    it "serializes models correctly" $ property $
        \x -> (stringToModel . modelToString) x == x
    it "lookups to names correctly" $ property $
        \(a::String) (b::Int) -> DM.lookup a (toName $ addToLookup a b def) == Just b
    it "lookups from names correctly" $ property $
        \(a::String) (b::Int) -> DM.lookup b (fromName $ addToLookup a b def) == Just a

instance (Ord a, Ord b,Arbitrary a, Arbitrary b)=> Arbitrary (Lookup a b) where
  arbitrary = lookupFromList <$> arbitrary

instance Arbitrary Model where
    arbitrary = Model <$> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary DataType where
    arbitrary = oneof [return DTText, return DTInteger, return DTBinary]
