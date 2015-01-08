module Main where

import Criterion.Main
import qualified Data.Map.Strict as DM

import Database.Graph.HGraphStorage.HackageTest
import Database.Graph.HGraphStorage.Types
import Data.Default (def)
import System.IO (BufferMode(..))


main :: IO()
main = do
  -- convertHack
  g <- buildHackageGraph
  let _ = g `seq` ()
  putStrLn (show (length $ DM.keys g) ++ " packages")
  defaultMain 
    [bench "Write Hackage Default Buffering" $ nfIO $ writeGraph def g
    ,bench "Write Hackage No Buffering" $ nfIO $ writeGraph def{gsMainBuffering = Just NoBuffering} g
    ,bench "Write Hackage Buffering 1024" $ nfIO $ writeGraph def{gsMainBuffering = Just $ BlockBuffering $ Just 1024} g
    ,bench "Write Hackage Buffering 4096" $ nfIO $ writeGraph def{gsMainBuffering = Just $ BlockBuffering $ Just 4096} g
    ,bench "Write Hackage Buffering 8192" $ nfIO $ writeGraph def{gsMainBuffering = Just $ BlockBuffering $ Just 8192} g
    ,bench "Index Lookup" $ nfIO $ nameIndex g]

