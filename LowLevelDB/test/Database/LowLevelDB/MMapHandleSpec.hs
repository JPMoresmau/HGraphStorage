{-# LANGUAGE ScopedTypeVariables, OverloadedStrings #-}
-- | Test MMap Handles
module Database.LowLevelDB.MMapHandleSpec where

import Database.LowLevelDB.MMapHandle
import Database.LowLevelDB.TestUtils

import Test.Hspec
import Foreign.Storable
import Data.Int

import Control.Monad
import Data.Word

import Control.Concurrent.Async

spec :: Spec
spec = describe "MMap tests" $ do
  it "MMap operations" $ do
      withTempFile "mmap" $ \f->do
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
      withTempFile "mmap" $ \f->do
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
  it "works with multiple threads"$ do
      withTempFile "mmap" $ \f->do
          let d0=24::Int64
          mm <- openMmap f (0,4096) d0
          let sz = sizeOf d0
          asyncs <- forM [1..10] $ \n-> async $ 25 `timesDo` writeOp mm  n sz
          forM_ asyncs wait
          forM [1..10] $ \n -> do
            d1::Int64<-peekMM mm (sz * n)
            d1 `shouldBe` fromInteger (toInteger n)
          closeMmap mm
          mm2 <- openMmap f (0,4096) d0
          forM [1..10] $ \n -> do
            d1::Int64<-peekMM mm2 (sz * n)
            d1 `shouldBe` fromInteger (toInteger n)
          closeMmap mm2

writeOp :: MMapHandle Int64 -> Int -> Int -> IO()
writeOp mm n sz= do
  pokeMM mm (fromInteger $ toInteger n) (sz * n)
  milliSleep 20
  return ()

