{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.TCache
import Database.Graph.TGraph

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import qualified Control.Concurrent.Thread as Th
import qualified Data.Text as T

main :: IO ()
main = do

  -- name <- atomically $ do
  --    newDBRef (PropertyType 1 DTText "name")
  -- ot1 <- atomically $ do
  --    p1 <- newDBRef (Property 1 name Nothing 0 5)
  --    newDBRef (ObjectType 1 (Just p1))
  -- syncCache
  -- let
  --   name = getDBRef "pt1" :: DBRef PropertyType
  --   ot1 = getDBRef "ot1" :: DBRef ObjectType
  --
  -- atomically (readDBRef name) >>= print
  -- atomically (readDBRef ot1) >>= print

  let rmdl = getDBRef modelName
  -- ot1 <- atomically $ getObjectTypeByName rmdl "type1"
  -- ot2 <- atomically $ getObjectTypeByName rmdl "type2"
  -- syncCache
  -- print ot1
  -- print ot2
  -- ot3 <- atomically $ getObjectTypeByName rmdl "type1"
  -- print ot3
  tids <- forM [1..10] $ \n-> Th.forkIO $ 25 `timesDo` doStuff rmdl (T.pack ("type" ++ show n))
  forM_ tids snd
  milliSleep 1000
  ots <- atomically $ do
     mdl <- getDefRef rmdl
     return $ mObjectTypes mdl

  print ots
  syncCache

timesDo = replicateM_
milliSleep = threadDelay . (*) 1000

doStuff rmdl n = do
  --print n
  ot <- atomically $ do
      ot <- getObjectTypeByName rmdl n
      getObjectTypeByID rmdl (otID ot)
  --print ot
  milliSleep 20
  return ()
