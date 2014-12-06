{-# LANGUAGE OverloadedStrings, OverloadedLists #-}
module Main where

import Database.Graph.HGraphStorage.API
import Database.Graph.HGraphStorage.Types
import System.Directory
import System.FilePath
import qualified Data.Map as DM
import Control.Monad.Logger (runStdoutLoggingT)
import Control.Monad (when, filterM, liftM)
import Control.Monad.IO.Class (liftIO)

main::IO()
main = do
  tmp <- getTemporaryDirectory
  --hs <- open (tmp </> "graph")
  --mdl <- readModel hs
  --print mdl
  --close hs
  let dir = tmp </> "graph"
  ex <- doesDirectoryExist dir
  print ex
  when ex $ do
    cnts <- getDirectoryContents dir
    mapM_ removeFile =<< (filterM doesFileExist $ map (dir </>) cnts)
  res <- runStdoutLoggingT $ withGraphStorage dir $ do
    th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
    liftIO $ print th
    fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
    liftIO $ print fg
    filterObjects (const True)
  print res