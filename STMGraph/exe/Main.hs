
module Main (
    main
) where

import Control.Monad
import Data.Monoid
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Concurrent.STM.TQueue
import           System.Directory
import           System.FilePath
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import GHC.Conc

main :: IO ()
main = do
    let dir=".data"
    ioQueue <- newChan
    as <- async $ ioHandler dir ioQueue
    writeChan ioQueue (Write "a" "valuea")
    writeChan ioQueue (Write "b" "valueb")
    v <- readValue "a" ioQueue
    print v
    writeChan ioQueue End
    wait as

readValue i ioQueue = do
    tv <- newEmptyMVar
    writeChan ioQueue (Read "a" tv)
    takeMVar tv
    -- return "fail"

ioHandler dir ioQueue= do
  ex <- doesDirectoryExist dir
  when ex $ removeDirectoryRecursive dir
  createDirectory dir
  listen dir ioQueue

listen dir ioQueue = do
  ioReq <- readChan ioQueue
  processReq dir ioQueue ioReq

processReq dir ioQueue (Read i tv) = do
    print $ "received read " <> (show i)
    s <- readFile (dir </> i)
    putMVar tv s
    listen dir ioQueue
processReq dir ioQueue (Write i v) = do
    print $ "received write " <> (show i)
    writeFile (dir </> i) v
    listen dir ioQueue
processReq _ _ End = return ()

type ID = String
type Value = String

data Request = Read ID (MVar Value)
    | Write ID Value
    | End

