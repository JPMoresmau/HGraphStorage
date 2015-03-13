{-# LANGUAGE ScopedTypeVariables, OverloadedStrings #-}
module Database.Graph.HGraphStorage.LowLevel.MMapTest where


import Test.Tasty
import Test.Tasty.HUnit
import Foreign.Storable
import Data.Int
import Database.Graph.HGraphStorage.LowLevel.MMapHandle
import Control.Monad
import System.FilePath
import System.Directory
import Data.Word


mmapTests :: TestTree
mmapTests = testGroup "MMap tests"
  [ testCase "MMap operations" $ do
      tmp <- getTemporaryDirectory
      let f = tmp </> "mmap"
      ex <- doesFileExist f
      when ex $ removeFile f
      let d0=24::Int64
      mm <- openMmap f (0,4096) d0
      let sz = sizeOf d0
      _<-foldM (\idx i->do
          pokeMM mm i idx
          return $ idx+sz
        ) 0 ([0..2560]::[Int64])
      closeMM mm
      mm2 <- openMmap f (0,4096) d0
      d1::Int64<-peekMM mm2 0
      d2::Int64<-peekMM mm2 (sz*2500)
      closeMM mm2
      d1 @?= 0
      d2 @?= 2500
  , testCase "MMap Bytestring" $ do
      tmp <- getTemporaryDirectory
      let f = tmp </> "mmap"
      ex <- doesFileExist f
      when ex $ removeFile f
      let d0=24::Word8
      mm <- openMmap f (0,4096) d0
      pokeMMBS mm "hello mmap" 0
      pokeMMBS mm "test" 10
      closeMM mm
      mm2 <- openMmap f (0,4096) d0
      bs1 <- peekMMBS mm2 0 10
      bs2 <- peekMMBS mm2 10 4
      closeMM mm2
      bs1 @?= "hello mmap"
      bs2 @?= "test"
  ]