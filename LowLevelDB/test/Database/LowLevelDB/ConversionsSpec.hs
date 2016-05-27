{-# LANGUAGE ScopedTypeVariables #-}
--  | Test conversions
module Database.LowLevelDB.ConversionsSpec where

import Database.LowLevelDB.Conversions

import Data.Int

import Test.Hspec
import Test.QuickCheck

spec :: Spec
spec = do
  describe "Conversions operations" $ do
    it "round trips word8 correctly" $ property $
        \(Positive (x::Int64)) -> (fromWord8s . toWord8s) x == x
    it "round trips word4 correctly" $ property $
        \(Positive (x::Int64)) -> (fromWord4s . toWord4s) x == x
    it "round trips bits correctly" $ property $
        \(Positive (x::Int64)) -> (fromBits . toBits) x == x
    it "converts word8 correctly" $ do
        toWord8s (0::Int64) `shouldBe` []
        toWord8s (1::Int64) `shouldBe` [1]
        toWord8s (255::Int64) `shouldBe` [255]
        toWord8s (256::Int64) `shouldBe` [0,1]
        toWord8s (258::Int64) `shouldBe` [2,1]
    it "converts word4 correctly" $ do
        toWord4s (0::Int64) `shouldBe` []
        toWord4s (1::Int64) `shouldBe` [1]
        toWord4s (15::Int64) `shouldBe` [15]
        toWord4s (16::Int64) `shouldBe` [0,1]
        toWord4s (18::Int64) `shouldBe` [2,1]
    it "converts bit correctly" $ do
        toBits (0::Int64) `shouldBe` []
        toBits (1::Int64) `shouldBe` [1]
        toBits (2::Int64) `shouldBe` [0,1]
        toBits (4::Int64) `shouldBe` [0,0,1]
