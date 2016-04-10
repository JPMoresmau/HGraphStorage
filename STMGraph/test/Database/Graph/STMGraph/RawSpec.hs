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

import Database.Graph.STMGraph.Constants
import Database.Graph.STMGraph.Raw
import Database.Graph.STMGraph.Types

import Control.Exception
import Control.Monad
import Control.Monad.STM

import Data.Default

import System.Directory

import System.FilePath
import Control.Concurrent.Async

import System.Posix
import System.FilePath

import qualified Data.Text as T

spec :: Spec
spec = do
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
    it "modifies concurrently the model" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        mdl0 <- atomically $ getModel db0
        mdl0 `shouldBe` def
        let f n mdl = mdl{mObjectTypes=addToLookup n (T.pack  ("type"++show n)) (mObjectTypes mdl)}
        let modelOp db n = atomically $ updateModel (f n) db
        asyncs <- forM [1..10] $ \n-> async $ 25 `replicateM_` modelOp db0 n
        forM_ asyncs wait
        mdl1 <- atomically $ getModel db0
        let mdl1Exp = foldr f mdl0 [1..10]
        mdl1 `shouldBe` mdl1Exp
        close db0
        db1 <-  open dir def
        mdl2 <- atomically $ getModel db1
        mdl2 `shouldBe` mdl1Exp
        close db1
  describe "Basic operations" $ do
    it "provides object operations" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        let o0= Object 1 2 3 4
        oid0 <- atomically $ writeObject db0 Nothing o0
        oid0 `shouldBe`  1
        o1 <- atomically $ readObject db0 oid0
        o1 `shouldBe` o0
        let o2= Object 5 6 7 8
        oid1 <- atomically $ writeObject db0 (Just oid0) o2
        oid1 `shouldBe`  oid0
        o3 <- atomically $ readObject db0 oid0
        o3 `shouldBe` o2
        atomically $ deleteObject db0 oid0
        oid2 <- atomically $ writeObject db0 Nothing o0
        oid2 `shouldBe`  1
        atomically $ deleteObject db0 oid2
        close db0
    it "provides relation operations" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        let o0= Relation 1 2 3 4 5 6 7 8
        oid0 <- atomically $ writeRelation db0 Nothing o0
        oid0 `shouldBe`  1
        o1 <- atomically $ readRelation db0 oid0
        o1 `shouldBe` o0
        let o2= Relation 8 7 6 5 4 3 2 1
        oid1 <- atomically $ writeRelation db0 (Just oid0) o2
        oid1 `shouldBe`  oid0
        o3 <- atomically $ readRelation db0 oid0
        o3 `shouldBe` o2
        atomically $ deleteRelation db0 oid0
        oid2 <- atomically $ writeRelation db0 Nothing o0
        oid2 `shouldBe`  1
        atomically $ deleteRelation db0 oid2
        close db0
    it "provides property operations" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        let p0=Property 1 2 0 5
        let v0=PVText "hello"
        pid0 <- atomically $ writeProperty db0 Nothing ((1,2),v0)
        pid0 `shouldBe` 1
        pv1 <- atomically $ readProperty db0 pid0
        pv1 `shouldBe` (p0,v0)
        let v1=PVText "world"
        pid1 <- atomically $ writeProperty db0 (Just pid0) ((1,2),v1)
        pid1 `shouldBe` pid0
        pv2 <- atomically $ readProperty db0 pid1
        pv2 `shouldBe` (p0,v1)
        let v2=PVText "longer"
        pid2 <- atomically $ writeProperty db0 (Just pid0) ((1,2),v2)
        pid2 `shouldBe` pid0
        pv3 <- atomically $ readProperty db0 pid2
        pv3 `shouldBe` (Property 1 2 0 6,v2)
        atomically $ deleteProperty db0 pid2
        pid3 <- atomically $ writeProperty db0 Nothing ((1,2),v0)
        pid3 `shouldBe` 1
        pv4 <- atomically $ readProperty db0 pid3
        pv4 `shouldBe` (p0,v0)
        atomically $ deleteProperty db0 pid3
        close db0
  describe "Misc" $ do
    it "supports checkpointing" $ do
        dir <- testEmptyDir
        db0 <- open dir def
        let objF = dir </> objectFile
        ms0 <- getFileSize objF
        ms0 `shouldBe` 0
        let o0= Object 1 2 3 4
        oid0 <- atomically $ writeObject db0 Nothing o0
        checkpoint db0
        ms1 <- getFileSize objF
        ms1 `shouldBe` (COff objectSize)
        atomically $ deleteObject db0 oid0
        close db0

testEmptyDir = do
    tmp<-getTemporaryDirectory
    let dir = tmp </> "STMGraph.RawSpec"
    ex <- doesDirectoryExist dir
    when ex $
        removeDirectoryRecursive dir
    return dir

-- |  <http://stackoverflow.com/questions/5620332/what-is-the-best-way-to-retrieve-the-size-of-a-file-in-haskell>
getFileSize :: String -> IO FileOffset
getFileSize path = fileSize <$> getFileStatus path
