{-# LANGUAGE ScopedTypeVariables #-}
--  | Test conversions
module Database.LowLevelDB.ConversionsSpec where

import Database.LowLevelDB.Conversions

import Data.Word
import Data.List

import Test.Hspec
import Test.QuickCheck

spec :: Spec
spec = do
  describe "Conversions operations" $ do
    it "round trips word8 correctly" $ property $
        \(Positive (x::Word64)) -> (fromWord8s . toWord8s) x == x
    it "round trips word4 correctly" $ property $
        \(Positive (x::Word64)) -> (fromWord4s . toWord4s) x == x
    it "round trips bits correctly" $ property $
        \(Positive (x::Word64)) -> (fromBits . toBits) x == x
    it "round trips Word64 TxKey correctly" $ property $
         \(Positive (x::Word64),Positive (y::Word64)) -> (fromKey . toKey) (x,y) == (x,y)
    it "prefixes Word64 TxKey keyPrefix correctly" $ property $
        \(Positive (x::Word64)) -> toWord4s x `isPrefixOf` keyPrefix x
    it "prefixes Word64 TxKey key correctly" $ property $
        \(Positive (x::Word64),Positive (y::Word64)) -> keyPrefix x `isPrefixOf` toKey (x,y)
    it "converts word8 correctly" $ do
        toWord8s (0::Word64) `shouldBe` []
        toWord8s (1::Word64) `shouldBe` [1]
        toWord8s (255::Word64) `shouldBe` [255]
        toWord8s (256::Word64) `shouldBe` [0,1]
        toWord8s (258::Word64) `shouldBe` [2,1]
    it "converts word4 correctly" $ do
        toWord4s (0::Word64) `shouldBe` []
        toWord4s (1::Word64) `shouldBe` [1]
        toWord4s (15::Word64) `shouldBe` [15]
        toWord4s (16::Word64) `shouldBe` [0,1]
        toWord4s (18::Word64) `shouldBe` [2,1]
    it "converts bit correctly" $ do
        toBits (0::Word64) `shouldBe` []
        toBits (1::Word64) `shouldBe` [1]
        toBits (2::Word64) `shouldBe` [0,1]
        toBits (4::Word64) `shouldBe` [0,0,1]
