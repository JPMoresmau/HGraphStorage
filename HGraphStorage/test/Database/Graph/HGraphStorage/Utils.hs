{-# LANGUAGE ConstraintKinds, FlexibleContexts, RankNTypes #-}
-- | test utilities
module Database.Graph.HGraphStorage.Utils where


import Database.Graph.HGraphStorage.API
import System.Directory
import System.FilePath
import Control.Monad (when, filterM)
import qualified Control.Monad.Trans.Resource as R

import Control.Monad.Logger

withTempDB :: forall b.
                GraphStorageT (R.ResourceT (LoggingT IO)) b
                -> IO b
withTempDB f = do
  tmp <- getTemporaryDirectory
  let dir = tmp </> "graph-test"
  ex <- doesDirectoryExist dir
  when ex $ do
    cnts <- getDirectoryContents dir
    mapM_ removeFile =<< filterM doesFileExist (map (dir </>) cnts)
  runStderrLoggingT $ withGraphStorage dir f
  
withTempFile :: (FilePath -> IO b)
                -> IO b
withTempFile func = do
  tmp <- getTemporaryDirectory
  let f = tmp </> "graph-test.tmp"
  ex <- doesFileExist f
  when ex $ removeFile f
  b<-func f
  ex2 <- doesFileExist f
  when ex2 $ removeFile f
  return b
