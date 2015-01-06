module Main where

import Criterion.Main
import qualified Data.Map.Strict as DM

import Database.Graph.HGraphStorage.HackageTest


main :: IO()
main = do
  -- convertHack
  g <- buildHackageGraph
  let _ = g `seq` ()
  putStrLn $ ((show $ length $ DM.keys g) ++  " packages")
  defaultMain 
    [bench "Write Hackage" $ nfIO $ writeGraph g]

