{-# LANGUAGE ScopedTypeVariables, OverloadedStrings #-}
-- | Test MMap Handles
module Database.Graph.STMGraph.LowLevel.MMapHandleSpec where


import Test.Hspec
import Foreign.Storable
import Data.Int
import Database.Graph.STMGraph.LowLevel.MMapHandle
import Control.Monad
import System.FilePath
import System.Directory
import Data.Word


spec :: Spec
spec = describe "MMap tests" $ do
  it "MMap operations" $ do
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
      closeMmap mm
      mm2 <- openMmap f (0,4096) d0
      d1::Int64<-peekMM mm2 0
      d2::Int64<-peekMM mm2 (sz*2500)
      closeMmap mm2
      d1 `shouldBe` 0
      d2 `shouldBe` 2500
  it "MMap Bytestring" $ do
      tmp <- getTemporaryDirectory
      let f = tmp </> "mmap"
      ex <- doesFileExist f
      when ex $ removeFile f
      let d0=24::Word8
      mm <- openMmap f (0,4096) d0
      pokeMMBS mm "hello mmap" 0
      pokeMMBS mm "test" 10
      closeMmap mm
      mm2 <- openMmap f (0,4096) d0
      bs1 <- peekMMBS mm2 0 10
      bs2 <- peekMMBS mm2 10 4
      closeMmap mm2
      bs1 `shouldBe` "hello mmap"
      bs2 `shouldBe` "test"

