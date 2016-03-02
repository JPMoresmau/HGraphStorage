{-# LANGUAGE OverloadedStrings #-}
module Database.Graph.TGraph.DataSpec where

import Database.Graph.TGraph.ModelSpec

import System.Directory
import Test.Hspec

import Data.List
import Data.Maybe
import Data.Monoid
import Data.TCache
import Database.Graph.TGraph
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import qualified Control.Concurrent.Thread as Th
import qualified Data.Text as T
import Control.Concurrent.MVar

spec :: Spec
spec =
  describe "Data Operations" $ do
    describe "Properties" $ do
      it "Can add one property" $ withEmptyData $ \md -> do
        pt <- atomically $ getPropertyTypeByName (mdModel md) ("name",DTText)
        pRef1 <- atomically $ newProperty (ptID pt) Nothing (PVText "value1") md
        -- let k = keyObjDBRef pRef1
        mp1 <- atomically $ readDBRef pRef1
        isJust mp1 `shouldBe` True
        let Just p1 = mp1
        pID p1 `shouldBe` (0,33)
        pValue p1 `shouldBe` PVText "value1"
        pType p1 `shouldBe` ptID pt
        pNext p1 `shouldBe` Nothing
        syncCache
        atomically flushAll
        mp1' <- atomically $ readDBRef pRef1
        isJust mp1' `shouldBe` True
        let Just p1' = mp1'
        pID p1' `shouldBe` (0,33)
        pValue p1' `shouldBe` PVText "value1"
        pType p1' `shouldBe` ptID pt
        pNext p1' `shouldBe` Nothing
        let pRef1' = getDBRef $ toStringID $ pID p1'
        mp1'' <- atomically $ readDBRef pRef1'
        isJust mp1'' `shouldBe` True
        let Just p1'' = mp1''
        pID p1'' `shouldBe` (0,33)
        pValue p1'' `shouldBe` PVText "value1"
        pType p1'' `shouldBe` ptID pt
        pNext p1'' `shouldBe` Nothing
      it "Can add several properties" $ withEmptyData $ \md -> do
        pt <- atomically $ getPropertyTypeByName (mdModel md) ("name",DTText)
        pRef1 <- atomically $ newProperty (ptID pt) Nothing (PVText "value1") md
        mp1 <- atomically $ readDBRef pRef1
        isJust mp1 `shouldBe` True
        let Just p1 = mp1
        pID p1 `shouldBe` (0,33)
        pValue p1 `shouldBe` PVText "value1"
        pType p1 `shouldBe` ptID pt
        pNext p1 `shouldBe` Nothing
        pRef2 <- atomically $ newProperty (ptID pt) (Just pRef1) (PVText "value1b") md
        mp2 <- atomically $ readDBRef pRef2
        isJust mp2 `shouldBe` True
        let Just p2 = mp2
        pID p2 `shouldBe` (33,34)
        pValue p2 `shouldBe` PVText "value1b"
        pType p2 `shouldBe` ptID pt
        pNext p2 `shouldBe` Just pRef1
      it "Can handle concurrent properties" $ withEmptyData $ \md -> do
        let nb = 10
        refs <- atomically $ newTVar []
        let propOp n = do
                        -- print $ "in: " ++ (show n)
                        atomically $ do
                              pt<- getPropertyTypeByName (mdModel md) ("name",DTText)
                              p <- newProperty (ptID pt) Nothing (PVText ("value" <> T.pack (show n))) md
                              modifyTVar refs (\l->p:l)
                              return ()
                        milliSleep 20
                        return ()
        forks propOp nb
        -- ps<-atomically $ readTVar refs
        -- forM_ ps $ \p-> do
        --   mp2 <- atomically $ readDBRef p
        --   isJust mp2 `shouldBe` True



withEmptyData :: (MetaData -> IO()) -> IO ()
withEmptyData f = withEmptyModel $ \rmdl -> do
  hs <- open testDir
  setHandles hs
  f metaData
  close hs
