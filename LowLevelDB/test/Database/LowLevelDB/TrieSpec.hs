{-# LANGUAGE ScopedTypeVariables,OverloadedStrings #-}
-----------------------------------------------------------------------------
--
-- Module      :  Database.LowLevelDB.TrieSpec
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

module Database.LowLevelDB.TrieSpec where
import Database.LowLevelDB.MMapHandle
import Database.LowLevelDB.Conversions
import Database.LowLevelDB.Trie as T
import Database.LowLevelDB.TestUtils
import Data.Int
import Test.Hspec
import qualified Data.Text as T
import Data.Word


spec :: Spec
spec = describe "Trie tests" $ do
  it "Trie test" $
     withTempFile "trie" $ \f -> do
      tr :: (Trie Int16 Int16) <- newFileTrie f
      T.lookup (toInt16 "A") tr >>= (`shouldBe`  Nothing)
      T.lookup (toInt16 "to") tr >>= (`shouldBe` Nothing)
      insertNew (toInt16 "A") 15 tr >>= (`shouldBe` Nothing)
      T.lookup (toInt16 "A") tr >>= (`shouldBe` (Just 15))
      (Nothing `shouldBe`) =<< T.lookup (toInt16 "to") tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "tea") 3 tr
      equalsM (Just 3) $ T.lookup (toInt16 "tea") tr
      -- insert another value: ignored
      ((Just 15) `shouldBe`) =<<insertNew (toInt16 "A") 16 tr
      ((Just 15) `shouldBe`) =<<T.lookup (toInt16 "A") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "to") tr
      equalsM (Just 3) $ insertNew (toInt16 "tea") 3 tr
      ((Just 15) `shouldBe`) =<<T.lookup (toInt16 "A") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "to") tr
      equalsM (Just 3) $ T.lookup (toInt16 "tea") tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "ted") 4 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "ten") 12 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "to") 7 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "in") 5 tr
      (Nothing `shouldBe`) =<<insert (toInt16 "inn") 9 tr
      (Nothing `shouldBe`) =<<insert (toInt16 "i") 11 tr
      ((Just 15) `shouldBe`) =<<T.lookup (toInt16 "A") tr
      equalsM (Just 3) $ T.lookup (toInt16 "tea") tr
      equalsM (Just 4) $ T.lookup (toInt16 "ted") tr
      equalsM (Just 12) $ T.lookup (toInt16 "ten") tr
      equalsM (Just 7) $ T.lookup (toInt16 "to") tr
      equalsM (Just 5) $ T.lookup (toInt16 "in") tr
      equalsM (Just 9) $ T.lookup (toInt16 "inn") tr
      equalsM (Just 11) $ T.lookup (toInt16 "i") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "none") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "t") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "te") tr
      -- insert and override
      ((Just 15) `shouldBe`) =<<insert (toInt16 "A") 16 tr
      equalsM (Just 16) $ T.lookup (toInt16 "A") tr
      -- delete
      equalsM (Just 5) $ T.delete (toInt16 "in") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "in") tr
      equalsM (Just 9) $ T.lookup (toInt16 "inn") tr
      equalsM (Just 11) $ T.lookup (toInt16 "i") tr
      closeMmap $ trHandle tr
  it "Collision test" $
     withTempFile "trie" $ \f -> do
      tr :: (Trie Int16 Int32) <- newFileTrie f
      (Nothing `shouldBe`) =<<insert (toInt16 "3d-graphics-examples") 1 tr
      equalsM (Just 1) $ T.lookup (toInt16 "3d-graphics-examples") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "ace") tr
      (Nothing `shouldBe`) =<<T.lookup (toInt16 "ac-machine") tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "ac-machine") 945 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "ac-machine-conduit") 946 tr
      equalsM (Just 945) $ T.lookup (toInt16 "ac-machine") tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "accelerate-fourier-benchmark") 956 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "ace") 961 tr
      equalsM (Just 961) $ T.lookup (toInt16 "ace") tr
      equalsM (Just 945) $ T.lookup (toInt16 "ac-machine") tr
      closeMmap $ trHandle tr
  it "Prefix test" $
     withTempFile "trie" $ \f -> do
      tr :: (Trie Int16 Int16) <- newFileTrie f
      (Nothing `shouldBe`) =<<insertNew (toInt16 "A") 15 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "tea") 3 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "ted") 4 tr
      (Nothing `shouldBe`) =<<insertNew (toInt16 "to") 7 tr
      equalsM [] $ prefix (toInt16 "AB") tr
      equalsM [((toInt16 "tea"),3)] $ prefix (toInt16 "tea") tr
      equalsM [((toInt16 "tea"),3),((toInt16 "ted"),4)] $ prefix (toInt16 "te") tr
      equalsM [((toInt16 "tea"),3),((toInt16 "ted"),4),((toInt16 "to"),7)] $ prefix (toInt16 "t") tr
      equalsM [((toInt16 "A"),15),((toInt16 "tea"),3),((toInt16 "ted"),4),((toInt16 "to"),7)] $ prefix [] tr
      closeMmap $ trHandle tr
  it "Int64 keys as Word8" $
      withTempFile "trie" $ \f -> do
        tr :: (Trie Word8 Int16) <- newFileTrie f
        insertNew (toWord8s (100::Int64)) 15 tr >>= (`shouldBe` Nothing)
        T.lookup (toWord8s (100::Int64)) tr >>= (`shouldBe` (Just 15))
        insertNew (toWord8s (1000::Int64)) 25 tr >>= (`shouldBe` Nothing)
        T.lookup (toWord8s (1000::Int64)) tr >>= (`shouldBe` (Just 25))
        closeMmap $ trHandle tr
  it "Int64 keys as Bits" $
      withTempFile "trie" $ \f -> do
        tr :: (Trie Word8 Int16) <- newFileTrie f
        insertNew (toBits (100::Int64)) 15 tr >>= (`shouldBe` Nothing)
        T.lookup (toBits (100::Int64)) tr >>= (`shouldBe` (Just 15))
        insertNew (toBits (1000::Int64)) 25 tr >>= (`shouldBe` Nothing)
        T.lookup (toBits (1000::Int64)) tr >>= (`shouldBe` (Just 25))
        closeMmap $ trHandle tr
  it "Int64 keys as Word4" $
      withTempFile "trie" $ \f -> do
        tr :: (Trie Word8 Int16) <- newFileTrie f
        insertNew (toWord4s (100::Int64)) 15 tr >>= (`shouldBe` Nothing)
        T.lookup (toWord4s (100::Int64)) tr >>= (`shouldBe` (Just 15))
        insertNew (toWord4s (1000::Int64)) 25 tr >>= (`shouldBe` Nothing)
        T.lookup (toWord4s (1000::Int64)) tr >>= (`shouldBe` (Just 25))
        closeMmap $ trHandle tr


-- | equals assertion in the monad
equalsM :: (Show a, Eq a) =>
             a -> IO a -> IO ()
equalsM a f = (a `shouldBe`) =<< f


-- | Convert a text to an array of int16
toInt16 :: T.Text -> [Int16]
toInt16 = map (fromIntegral . fromEnum) . T.unpack

