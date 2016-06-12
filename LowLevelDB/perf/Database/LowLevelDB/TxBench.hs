{-# LANGUAGE ScopedTypeVariables,OverloadedStrings #-}
-- | Trie benchmark
module Database.LowLevelDB.TxBench where

import Database.LowLevelDB
import Data.Int
import qualified Data.Text as T
import Control.Monad
import System.FilePath
import System.Directory

resetFile :: FilePath -> IO()
resetFile f = do
  ex <- doesFileExist f
  when ex $ removeFile f

benchWord :: FilePath -> [T.Text] -> IO Int
benchWord fp wrds = do
  resetFile fp
  let fp2 = takeDirectory fp </> (takeFileName fp ++ "2")
  resetFile fp2
  withTxManager (TxManagerOptions fp Nothing) $
    withTxTrie fp2 Nothing $ \tr -> do
      withTransaction $ do
        res<-foldM (\c (_,i)->txInsert tr i i >> return (c+1)) 0 $ zip wrds [(0::Int64)..]
        txCommit
        return res
