{-# LANGUAGE ScopedTypeVariables #-}
--  | Test conversions
module Database.LowLevelDB.ConversionsSpec where

import Database.LowLevelDB.Conversions

import Data.Int
import Data.List

import Test.Hspec
import Test.QuickCheck

spec :: Spec
spec = do
  describe "Conversions operations" $ do
    it "round trips word8 correctly" $ property $
        \(Positive (x::Int64)) -> (fromWord8s . toWord8s) x == x
    it "round trips word4 correctly" $ property $
        \(Positive (x::Int64)) -> (fromWord4s . toWord4s) x == x
    it "round trips word4 tuples correctly" $ property $
        \(Positive (x::Int64),Positive (y::Int64)) -> (tupleFromWord4s . tupleToWord4s) (x,y) == (x,y)
    it "prefixes word4 tuples correctly" $ property $
        \(Positive (x::Int64),Positive (y::Int64)) -> toWord4s x `isPrefixOf` tupleToWord4s (x,y)
    it "round trips word4 triples correctly" $ property $
        \(Positive (x::Int64),Positive (y::Int64),Positive (z::Int64)) -> (tripleFromWord4s . tripleToWord4s) (x,y,z) == (x,y,z)
    it "prefixes word4 triples correctly" $ property $
        \(Positive (x::Int64),Positive (y::Int64),Positive (z::Int64)) -> toWord4s x `isPrefixOf` tripleToWord4s (x,y,z)
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
    it "converts word4 tuples correctly" $ do
        let tws1=tupleToWord4s (1::Int64,2::Int64)
        tws1 `shouldBe` [1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2]
        let tws2=tupleToWord4s (1::Int64,3::Int64)
        head tws2 `shouldBe` head tws1
        let tws3=tupleToWord4s (1::Int64,256::Int64)
        head tws3 `shouldBe` head tws1
    it "converts bit correctly" $ do
        toBits (0::Int64) `shouldBe` []
        toBits (1::Int64) `shouldBe` [1]
        toBits (2::Int64) `shouldBe` [0,1]
        toBits (4::Int64) `shouldBe` [0,0,1]
