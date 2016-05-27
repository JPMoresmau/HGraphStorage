{-# LANGUAGE ScopedTypeVariables #-}
-- | Test free list
module Database.LowLevelDB.FreeListSpec where

import Database.LowLevelDB.FreeList
import Database.LowLevelDB.TestUtils

import Data.Int

import Test.Hspec

spec :: Spec
spec = describe "FreeList tests" $ do
 it "Free List ending empty" $
     withTempFile "freelist" $ \f -> do
      fl <- newFileFreeList f (1::Int8)
      m1 <- getFromFreeList fl
      m1 `shouldBe` Nothing
      addToFreeList 3 fl
      m2 <- getFromFreeList fl
      m2 `shouldBe` Just 3
      addToFreeList 4 fl
      addToFreeList 5 fl
      m3 <- getFromFreeList fl
      m3 `shouldBe` Just 5
      m4 <- getFromFreeList fl
      m4 `shouldBe` Just 4
      m5 <- getFromFreeList fl
      m5 `shouldBe` Nothing
      hasData <- closeFreeList fl
      hasData `shouldBe` False
 it "Free List ending with data" $
     withTempFile "freelist" $ \f -> do
      fl <- newFileFreeList f (1::Int8)
      m1 <- getFromFreeList fl
      m1 `shouldBe` Nothing
      addToFreeList 3 fl
      hasData <- closeFreeList fl
      hasData `shouldBe` True
