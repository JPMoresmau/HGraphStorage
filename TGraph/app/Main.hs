{-# LANGUAGE OverloadedStrings,DeriveGeneric #-}
module Main where

import           Data.TCache
import           Data.TCache.DefaultPersistence

import           Control.Concurrent
import           Control.Concurrent.STM
-- import qualified Control.Concurrent.Thread as Th
import           Control.Monad
import           Data.Binary
import           Data.Binary.Put
import qualified Data.ByteString.Lazy                   as BS
import           Data.ByteString.Lazy.Char8             (pack,unpack)
import           Data.Monoid
import           Data.Default
import           Data.Int
import           Data.Typeable
import           GHC.Generics                           (Generic)
import           System.Directory


main :: IO ()
main = do
  ex <- doesDirectoryExist ".tcachedata"
  when ex $ removeDirectoryRecursive ".tcachedata"

  let md = getDBRef "maxIDs"
  print "start"
  tids <- forM [1..10] $ \n-> forkIO $ 25 `timesDo` propOp md n
  --forM_ tids snd
  getChar
  syncCache

timesDo = replicateM_
milliSleep = threadDelay . (*) 1000

-- | A simple IO doing STM
propOp :: DBRef MaxIDs -> Int -> IO()
propOp md n = do
  print $ "in: " ++ show n
  p <- atomically $ do
         pr <- newProperty "name" ("value" <> show n) md
         readDBRef pr
  print p
  milliSleep 20
  return ()

-- | Reads a DBRef of a default instance, if empty writes the default
getDefaultRef :: (Default a,Typeable a,IResource a) => DBRef a -> STM a
getDefaultRef rmdl = do
  mmdl <- readDBRef rmdl
  case mmdl of
    Just mdl -> return mdl
    Nothing -> do
      let mdl = def
      writeDBRef rmdl mdl
      return mdl

-- | One data kept in DBRef, this is a singleton
data MaxIDs = MaxIDs
  { miPropertyOffset :: Int64
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Serializable MaxIDs where
   serialize= pack . show
   deserialize= read . unpack

instance Indexable MaxIDs where
   key = const "maxIDs"

instance Default MaxIDs where
  def = MaxIDs def

-- | Another data kept as DB Ref
data Property = Property
  { pID :: Int64
  , pType :: String
  , pValue :: String
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Serializable Property where
   serialize= pack . show
   deserialize= read . unpack

instance Indexable Property where
   key p = "p" <> show (pID p)

-- | this reads/modifies the MaxIDs DBRef and create a new DBRef Property
newProperty :: String -> String -> DBRef MaxIDs -> STM (DBRef Property)
newProperty ptid pValue maxRef = do
  maxIds <- getDefaultRef maxRef
  let offset = miPropertyOffset maxIds
      b1 = runPut $ do
             put $ pack ptid
             put $ pack pValue
      valueLength = BS.length b1
  writeDBRef maxRef maxIds{miPropertyOffset=offset+valueLength}
  newDBRef $ Property offset ptid pValue
