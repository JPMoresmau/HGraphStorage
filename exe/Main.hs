module Main where

import System.Directory (getTemporaryDirectory)
import Database.Graph.HGraphStorage.FileOps
import System.FilePath ((</>))

main::IO()
main = do
  tmp <- getTemporaryDirectory
  hs <- open (tmp </> "graph")
  mdl <- readModel hs
  print mdl
  close hs