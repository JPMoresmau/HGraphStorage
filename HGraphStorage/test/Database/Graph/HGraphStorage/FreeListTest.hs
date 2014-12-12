{-# LANGUAGE ScopedTypeVariables #-}
module Database.Graph.HGraphStorage.FreeListTest where

import Database.Graph.HGraphStorage.FreeList
import Database.Graph.HGraphStorage.Utils

import Data.Int
import System.IO

import Test.Tasty
import Test.Tasty.HUnit

freeListTests :: TestTree
freeListTests = testGroup "Free List tests"
  [ testCase "Free List ending empty" $
     withTempFile $ \f -> do
      h <- openBinaryFile f ReadWriteMode
      (fl::FreeList Int8) <- initFreeList 1 h
      m1 <- getFromFreeList fl
      m1 @?= Nothing
      addToFreeList 3 fl
      m2 <- getFromFreeList fl
      m2 @?= Just 3
      addToFreeList 4 fl
      addToFreeList 5 fl
      m3 <- getFromFreeList fl
      m3 @?= Just 5
      m4 <- getFromFreeList fl
      m4 @?= Just 4
      m5 <- getFromFreeList fl
      m5 @?= Nothing
      hasData <- closeFreeList fl
      not hasData @? "has data at end" 
  , testCase "Free List ending with data" $
     withTempFile $ \f -> do
      h <- openBinaryFile f ReadWriteMode
      (fl::FreeList Int8) <- initFreeList 1 h
      m1 <- getFromFreeList fl
      m1 @?= Nothing
      addToFreeList 3 fl
      hasData <- closeFreeList fl
      hasData @? "has no data at end" 
  ]