module Main where

import Criterion.Main


import Database.LowLevelDB.TrieBench
import Database.LowLevelDB.TxBench
import System.Directory
import System.FilePath
import qualified Data.Text as T
import qualified Data.Text.IO as T


main :: IO()
main = do

  defaultMain [ env setupEnv $ \ ~(f,wrds) -> bgroup "main"
    [ --bench "Trie Value 16" $ nfIO $ benchTrieWordsValue16 f wrds
    --, bench "Trie Value 32" $ nfIO $ benchTrieWordsValue32 f wrds
    --, bench "Trie Value 64" $ nfIO $ benchTrieWordsValue64 f wrds
    --, bench "Trie Key Word8" $ nfIO $ benchTrieInt64AsWord8s f wrds
    --  , bench "Trie Key Bits" $ nfIO $ benchTrieInt64AsWord8s f wrds
    --,
      bench "Trie Key Word4" $ nfIO $ benchTrieInt64AsWord4s f wrds
    , bench "TxTrie Key Word4" $ nfIO $ benchWord f wrds
    ]]

setupEnv :: IO (FilePath,[T.Text])
setupEnv = do
  tmp <- getTemporaryDirectory
  let f = tmp </> "LowLevelDB.bench"
  wrds <- T.lines <$> T.readFile "perf/words.txt"
  let l=length wrds
  putStrLn $ "words:" ++  show l
  return (f,wrds)
