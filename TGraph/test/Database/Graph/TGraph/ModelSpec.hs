{-# LANGUAGE OverloadedStrings #-}
module Database.Graph.TGraph.ModelSpec where

import System.Directory
import Test.Hspec

import Data.Default
import Data.List
import Data.Monoid
import Data.TCache
import Database.Graph.TGraph
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import qualified Control.Concurrent.Thread as Th
import qualified Data.Text as T

spec :: Spec
spec =
  describe "Model Operations" $ do
    describe "Object Types" $ do
      it "Can add object types" $ withEmptyModel $ \rmdl -> do
        ots <- atomically $ getObjectTypes rmdl
        ots `shouldBe` []
        ot <- atomically $ getObjectTypeByName rmdl "type1"
        otID ot `shouldBe` 1
        otName ot `shouldBe` "type1"
        ot2 <- atomically $ getObjectTypeByName rmdl "type1"
        otID ot2 `shouldBe` 1
        otName ot2 `shouldBe` "type1"
        ot3 <- atomically $ getObjectTypeByID rmdl 1
        otID ot3 `shouldBe` 1
        otName ot3 `shouldBe` "type1"
        ots <- atomically $ getObjectTypes rmdl
        ots `shouldBe` [ObjectType 1 "type1"]
      it "Can handle concurrent object types" $ withEmptyModel $ \rmdl -> do
        let nb = 10
        let typeOp n = do
                        ot <- atomically $ do
                            ot <- getObjectTypeByName rmdl ("type" <> T.pack (show n))
                            getObjectTypeByID rmdl (otID ot)
                        milliSleep 20
                        return ()
        forks typeOp nb
        ots <- atomically $ getObjectTypes rmdl
        length ots `shouldBe` nb
        length (nub $ map otID ots) `shouldBe` nb
        length (nub $ map otName ots) `shouldBe` nb

withEmptyModel :: (DBRef Model -> IO()) -> IO ()
withEmptyModel f = do
  ex <- doesDirectoryExist testDir
  when ex $ removeDirectoryRecursive testDir
  atomically $ writeDBRef (getDBRef modelName) (def::Model)
  atomically $ writeDBRef (getDBRef maxName) (def::MaxIDs)
  let rmdl = getDBRef modelName
  f rmdl

testDir :: FilePath
testDir = ".tcachedata"

timesDo = replicateM_
milliSleep = threadDelay . (*) 1000

defaultRepeat = 25

forks doStuff nb = do
  tids <- forM [1..nb] $ \n-> Th.forkIO $ defaultRepeat `timesDo` doStuff n
  forM_ tids snd
