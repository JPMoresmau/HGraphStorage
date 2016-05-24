-----------------------------------------------------------------------------
--
-- Module      :  Database.LowLevelDB.TestUtils
-- Copyright   :
-- License     :  AllRightsReserved
--
-- Maintainer  :
-- Stability   :
-- Portability :
--
-- |
--
-----------------------------------------------------------------------------

module Database.LowLevelDB.TestUtils (
    milliSleep,
    timesDo,
    withTempFile
) where

import System.Directory
import System.FilePath
import           Control.Monad
import           Control.Concurrent

withTempFile :: FilePath -> (FilePath -> IO b)
                -> IO b
withTempFile n func = do
  tmp <- getTemporaryDirectory
  let f = tmp </> n
  ex <- doesFileExist f
  when ex $ removeFile f
  b<-func f
  ex2 <- doesFileExist f
  when ex2 $ removeFile f
  return b

timesDo :: (Monad m) => Int -> (m a) ->  m ()
timesDo = replicateM_

milliSleep :: Int -> IO()
milliSleep = threadDelay . (*) 1000
