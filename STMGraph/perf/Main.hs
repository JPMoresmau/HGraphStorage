module Main where

import Criterion.Main
import qualified Data.Map.Strict as DM

import Database.Graph.STMGraph.HackageTest


main :: IO()
main = do
  -- convertHack

  defaultMain [ env setupEnv $ \g -> bgroup "main"
    [bench "Write Hackage" $ nfIO $ writeGraph g
    --,bench "Index Lookup" $ nfIO $ nameIndex g
    --,bench "Index Lookup+One Step query" $ nfIO yesodQuery
    ]
    ]

setupEnv :: IO GraphMap
setupEnv = do
  g <- buildHackageGraph
  putStrLn (show (length $ DM.keys g) ++ " packages")
  return g
