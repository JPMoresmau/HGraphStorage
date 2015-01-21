{-# LANGUAGE ScopedTypeVariables, OverloadedStrings #-}
-- | Test index
module Database.Graph.HGraphStorage.IndexTest where

import Database.Graph.HGraphStorage.Index as GI
import Database.Graph.HGraphStorage.Utils

import Data.Int
import System.IO

import qualified Data.Text as T

import Test.Tasty
import Test.Tasty.HUnit

indexTests :: TestTree
indexTests = testGroup "Index tests"
  [ testCase "Trie test" $
     withTempFile $ \f -> do
      h <- openBinaryFile f ReadWriteMode
      let tr :: (Trie Int16 Int16) = newTrie h
      equalsM Nothing $ GI.lookup (toInt16 "A") tr
      equalsM Nothing $ GI.lookup (toInt16 "to") tr
      equalsM Nothing $ insertNew (toInt16 "A") 15 tr
      equalsM (Just 15) $ GI.lookup (toInt16 "A") tr
      equalsM Nothing $ GI.lookup (toInt16 "to") tr
      equalsM Nothing $ insertNew (toInt16 "tea") 3 tr
      equalsM (Just 3) $ GI.lookup (toInt16 "tea") tr
      -- insert another value: ignored
      equalsM (Just 15) $ insertNew (toInt16 "A") 16 tr
      equalsM (Just 15) $ GI.lookup (toInt16 "A") tr
      equalsM Nothing $ GI.lookup (toInt16 "to") tr
      equalsM (Just 3) $ insertNew (toInt16 "tea") 3 tr
      equalsM (Just 15) $ GI.lookup (toInt16 "A") tr
      equalsM Nothing $ GI.lookup (toInt16 "to") tr
      equalsM (Just 3) $ GI.lookup (toInt16 "tea") tr
      equalsM Nothing $ insertNew (toInt16 "ted") 4 tr
      equalsM Nothing $ insertNew (toInt16 "ten") 12 tr
      equalsM Nothing $ insertNew (toInt16 "to") 7 tr
      equalsM Nothing $ insertNew (toInt16 "in") 5 tr
      equalsM Nothing $ insert (toInt16 "inn") 9 tr
      equalsM Nothing $ insert (toInt16 "i") 11 tr
      equalsM (Just 15) $ GI.lookup (toInt16 "A") tr
      equalsM (Just 3) $ GI.lookup (toInt16 "tea") tr
      equalsM (Just 4) $ GI.lookup (toInt16 "ted") tr
      equalsM (Just 12) $ GI.lookup (toInt16 "ten") tr
      equalsM (Just 7) $ GI.lookup (toInt16 "to") tr
      equalsM (Just 5) $ GI.lookup (toInt16 "in") tr
      equalsM (Just 9) $ GI.lookup (toInt16 "inn") tr
      equalsM (Just 11) $ GI.lookup (toInt16 "i") tr
      equalsM Nothing $ GI.lookup (toInt16 "none") tr
      equalsM Nothing $ GI.lookup (toInt16 "t") tr
      equalsM Nothing $ GI.lookup (toInt16 "te") tr
      -- insert and override
      equalsM (Just 15) $ insert (toInt16 "A") 16 tr
      equalsM (Just 16) $ GI.lookup (toInt16 "A") tr
      -- delete
      equalsM (Just 5) $ GI.delete (toInt16 "in") tr
      equalsM Nothing $ GI.lookup (toInt16 "in") tr
      equalsM (Just 9) $ GI.lookup (toInt16 "inn") tr
      equalsM (Just 11) $ GI.lookup (toInt16 "i") tr
      hClose h
   , testCase "Collision test" $
     withTempFile $ \f -> do
      tr :: (Trie Int16 Int32) <- newFileTrie f
      equalsM Nothing $ insert (toInt16 "3d-graphics-examples") 1 tr
      equalsM (Just 1) $ GI.lookup (toInt16 "3d-graphics-examples") tr
      equalsM Nothing $ GI.lookup (toInt16 "ace") tr
      equalsM Nothing $ GI.lookup (toInt16 "ac-machine") tr
      equalsM Nothing $ insertNew (toInt16 "ac-machine") 945 tr
      equalsM Nothing $ insertNew (toInt16 "ac-machine-conduit") 946 tr
      equalsM (Just 945) $ GI.lookup (toInt16 "ac-machine") tr
      equalsM Nothing $ insertNew (toInt16 "accelerate-fourier-benchmark") 956 tr
      equalsM Nothing $ insertNew (toInt16 "ace") 961 tr
      equalsM (Just 961) $ GI.lookup (toInt16 "ace") tr
      equalsM (Just 945) $ GI.lookup (toInt16 "ac-machine") tr
      hClose $ trHandle tr
    , testCase "Prefix test" $
     withTempFile $ \f -> do
      h <- openBinaryFile f ReadWriteMode
      let tr :: (Trie Int16 Int16) = newTrie h
      equalsM Nothing $ insertNew (toInt16 "A") 15 tr
      equalsM Nothing $ insertNew (toInt16 "tea") 3 tr
      equalsM Nothing $ insertNew (toInt16 "ted") 4 tr
      equalsM Nothing $ insertNew (toInt16 "to") 7 tr
      equalsM [] $ prefix (toInt16 "AB") tr
      equalsM [((toInt16 "tea"),3)] $ prefix (toInt16 "tea") tr
      equalsM [((toInt16 "tea"),3),((toInt16 "ted"),4)] $ prefix (toInt16 "te") tr
      equalsM [((toInt16 "tea"),3),((toInt16 "ted"),4),((toInt16 "to"),7)] $ prefix (toInt16 "t") tr
      equalsM [((toInt16 "A"),15),((toInt16 "tea"),3),((toInt16 "ted"),4),((toInt16 "to"),7)] $ prefix [] tr
  ]
  

-- | equals assertion in the monad
equalsM :: (Show a, Eq a) =>
             a -> IO a -> IO ()
equalsM a f = (a @=?) =<< f


-- | Convert a string to an array of int16
toInt16 :: T.Text -> [Int16]
toInt16 = map (fromIntegral . fromEnum) . T.unpack
