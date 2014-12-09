{-# LANGUAGE OverloadedStrings, OverloadedLists #-}
module Main where

import Database.Graph.HGraphStorage.API
import Database.Graph.HGraphStorage.Query
import Database.Graph.HGraphStorage.Types
import System.Directory
import System.FilePath
import qualified Data.Map as DM
import Control.Monad.Logger (runStdoutLoggingT)
import Control.Monad (when, filterM, liftM)
import Control.Monad.IO.Class (liftIO)
import Data.Default (def)
import Data.Maybe (fromJust)


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
    (liftIO . print) =<< filterObjects (return . const True)
    fgp <- createRelation (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
    liftIO $ print fgp
    ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
    createRelation (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Somebody"])])
    --filterRelations (const True)
    queryStep (fromJust $ goID th) def
    --queryStep (fromJust $ goID ss) def{rsDirection=IN}
  print res