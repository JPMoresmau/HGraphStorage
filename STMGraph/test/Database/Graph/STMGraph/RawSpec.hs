{-# LANGUAGE OverloadedStrings #-}
-----------------------------------------------------------------------------
--
-- Module      :  Database.Graph.STMGraph.RawSpec
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

module Database.Graph.STMGraph.RawSpec where

import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Instances

import Database.Graph.STMGraph.Raw
import Database.Graph.STMGraph.Types

import Control.Monad
import Control.Monad.STM

import Data.Default

import System.Directory
import System.FilePath

spec :: Spec
spec =
  describe "Model operations" $ do
    it "modifies and saves model" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        mdl0 <- atomically $ getModel db0
        mdl0 `shouldBe` def
        let f mdl = mdl{mObjectTypes=addToLookup 0 "type0" (mObjectTypes mdl)}
        let mdl1Exp = f mdl0
        atomically $ updateModel f db0
        mdl1 <- atomically $ getModel db0
        mdl1 `shouldBe` mdl1Exp
        close db0
        db1 <-  open dir def
        mdl2 <- atomically $ getModel db1
        mdl2 `shouldBe` mdl1Exp
        close db1

testEmptyDir = do
    tmp<-getTemporaryDirectory
    let dir = tmp </> "STMGraph.RawSpec"
    ex <- doesDirectoryExist dir
    when ex $
        removeDirectoryRecursive dir
    return dir
