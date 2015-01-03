{-# LANGUAGE ScopedTypeVariables #-}
-- | Test index
module Database.Graph.HGraphStorage.IndexTest where

import Database.Graph.HGraphStorage.Index as GI
import Database.Graph.HGraphStorage.Utils

import Data.Int
import System.IO

import Test.Tasty
import Test.Tasty.HUnit
import Control.Monad (liftM, void)

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
      -- insert another value: ignored
      equalsM Nothing $ insertNew (toInt16 "A") 16 tr
      equalsM (Just 15) $ GI.lookup (toInt16 "A") tr
      equalsM Nothing $ GI.lookup (toInt16 "to") tr
      equalsM Nothing $ insertNew (toInt16 "tea") 3 tr
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
      equalsM Nothing $ insertNew (toInt16 "A") 16 tr
      equalsM (Just 16) $ GI.lookup (toInt16 "A") tr
      -- delete
      equalsM (Just 5) $ GI.delete (toInt16 "in") tr
      equalsM Nothing $ GI.lookup (toInt16 "in") tr
      equalsM (Just 9) $ GI.lookup (toInt16 "inn") tr
      equalsM (Just 11) $ GI.lookup (toInt16 "i") tr
      hClose h
  ]
  

-- | equals assertion in the monad
equalsM :: (Show a, Eq a) =>
             a -> IO a -> IO ()
equalsM a = void . (liftM $ (\x -> a @=? x))


-- | Convert a string to an array of int16
toInt16 :: String -> [Int16]
toInt16 = map (fromIntegral . fromEnum)
