{-# LANGUAGE ScopedTypeVariables,OverloadedStrings #-}
-- | Trie benchmark
module Database.LowLevelDB.TrieBench where

import Database.LowLevelDB.Conversions
import Database.LowLevelDB.MMapHandle
import Database.LowLevelDB.Trie
import Data.Int
import qualified Data.Text as T
import Control.Monad
import System.Directory
import Data.Word

resetFile :: FilePath -> IO()
resetFile f = do
  ex <- doesFileExist f
  when ex $ removeFile f

benchTrieWordsValue16 :: FilePath -> [T.Text] -> IO Int
benchTrieWordsValue16 f wrds= do
    resetFile f
    tr :: (Trie Int16 Int16) <- openFileTrie f Nothing
    res<-foldM (\c (w,i)->insertNew (toInt16 w) i tr >> return (c+1)) 0 $ zip wrds [0..]
    closeMmap $ trHandle tr
    return res

benchTrieWordsValue32 :: FilePath -> [T.Text] -> IO Int
benchTrieWordsValue32 f wrds= do
    resetFile f
    tr :: (Trie Int16 Int32) <- openFileTrie f Nothing
    res<-foldM (\c (w,i)->insertNew (toInt16 w) i tr >> return (c+1)) 0 $ zip wrds [0..]
    closeMmap $ trHandle tr
    return res

benchTrieWordsValue64 :: FilePath -> [T.Text] -> IO Int
benchTrieWordsValue64 f wrds= do
    resetFile f
    tr :: (Trie Int16 Int64) <- openFileTrie f Nothing
    res<-foldM (\c (w,i)->insertNew (toInt16 w) i tr >> return (c+1)) 0 $ zip wrds [0..]
    closeMmap $ trHandle tr
    return res

benchTrieInt64AsWord8s :: FilePath -> [T.Text] -> IO Int
benchTrieInt64AsWord8s = benchInt64 toWord8s

benchInt64 :: (Int64 -> [Word8]) -> FilePath -> [T.Text] -> IO (Int)
benchInt64 conv f wrds= do
    resetFile f
    tr :: (Trie Word8 Int64) <- openFileTrie f Nothing
    res<-foldM (\c (_,i)->insertNew (conv i) i tr >> return (c+1)) 0 $ zip wrds [0..]
    closeMmap $ trHandle tr
    return res

benchTrieInt64AsBits :: FilePath -> [T.Text] -> IO Int
benchTrieInt64AsBits= benchInt64 toBits


benchTrieInt64AsWord4s :: FilePath -> [T.Text] -> IO Int
benchTrieInt64AsWord4s = benchInt64 toWord4s

-- | Convert a text to an array of int16
toInt16 :: T.Text -> [Int16]
toInt16 = map (fromIntegral . fromEnum) . T.unpack

