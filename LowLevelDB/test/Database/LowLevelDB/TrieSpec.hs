{-# LANGUAGE ScopedTypeVariables,OverloadedStrings #-}
-- | Test tries
module Database.LowLevelDB.TrieSpec where
import Database.LowLevelDB.Conversions
import Database.LowLevelDB.Trie as T
import Database.LowLevelDB.TestUtils
import Data.Int
import Test.Hspec
import qualified Data.Text as T
import Data.Word
import Control.Concurrent.Async
import Control.Monad

spec :: Spec
spec = describe "Trie tests" $ do
  it "Trie test" $
     withTempFile "trie" $ \f -> do
      tr :: (Trie Int16 Int16) <- openFileTrie f Nothing
      T.lookup (toInt16 "A") tr `shouldReturn`  Nothing
      T.lookup (toInt16 "to") tr `shouldReturn` Nothing
      insertNew (toInt16 "A") 15 tr `shouldReturn` Nothing
      T.lookup (toInt16 "A") tr `shouldReturn` Just 15
      T.lookup (toInt16 "to") tr `shouldReturn` Nothing
      insertNew (toInt16 "tea") 3 tr `shouldReturn` Nothing
      T.lookup (toInt16 "tea") tr `shouldReturn` Just 3
      -- insert another value: ignored
      insertNew (toInt16 "A") 16 tr `shouldReturn` Just 15
      T.lookup (toInt16 "A") tr `shouldReturn` Just 15
      T.lookup (toInt16 "to") tr `shouldReturn` Nothing
      insertNew (toInt16 "tea") 3 tr `shouldReturn` Just 3
      T.lookup (toInt16 "A") tr `shouldReturn` Just 15
      T.lookup (toInt16 "to") tr `shouldReturn` Nothing
      T.lookup (toInt16 "tea") tr `shouldReturn` Just 3
      insertNew (toInt16 "ted") 4 tr `shouldReturn` Nothing
      insertNew (toInt16 "ten") 12 tr `shouldReturn` Nothing
      insertNew (toInt16 "to") 7 tr `shouldReturn` Nothing
      insertNew (toInt16 "in") 5 tr `shouldReturn` Nothing
      insert (toInt16 "inn") 9 tr `shouldReturn` Nothing
      insert (toInt16 "i") 11 tr `shouldReturn` Nothing
      T.lookup (toInt16 "A") tr `shouldReturn` Just 15
      T.lookup (toInt16 "tea") tr `shouldReturn` Just 3
      T.lookup (toInt16 "ted") tr `shouldReturn` Just 4
      T.lookup (toInt16 "ten") tr `shouldReturn`Just 12
      T.lookup (toInt16 "to") tr `shouldReturn` Just 7
      T.lookup (toInt16 "in") tr `shouldReturn` Just 5
      T.lookup (toInt16 "inn") tr `shouldReturn` Just 9
      T.lookup (toInt16 "i") tr `shouldReturn`Just 11
      T.lookup (toInt16 "none") tr `shouldReturn` Nothing
      T.lookup (toInt16 "t") tr `shouldReturn` Nothing
      T.lookup (toInt16 "te") tr `shouldReturn` Nothing
      -- insert and override
      insert (toInt16 "A") 16 tr `shouldReturn` Just 15
      T.lookup (toInt16 "A") tr `shouldReturn` Just 16
      -- delete
      T.delete (toInt16 "in") tr `shouldReturn` Just 5
      T.lookup (toInt16 "in") tr `shouldReturn` Nothing
      T.lookup (toInt16 "inn") tr `shouldReturn` Just 9
      T.lookup (toInt16 "i") tr `shouldReturn` Just 11
      T.delete (toInt16 "inn") tr `shouldReturn` Just 9
      T.delete (toInt16 "inn") tr `shouldReturn` Nothing
      closeTrie tr
  it "Collision test" $
     withTempFile "trie" $ \f -> do
      tr :: (Trie Int16 Int32) <- openFileTrie f Nothing
      insert (toInt16 "3d-graphics-examples") 1 tr `shouldReturn` Nothing
      T.lookup (toInt16 "3d-graphics-examples") tr `shouldReturn` Just 1
      T.lookup (toInt16 "ace") tr `shouldReturn` Nothing
      T.lookup (toInt16 "ac-machine") tr `shouldReturn` Nothing
      insertNew (toInt16 "ac-machine") 945 tr `shouldReturn` Nothing
      insertNew (toInt16 "ac-machine-conduit") 946 tr `shouldReturn` Nothing
      T.lookup (toInt16 "ac-machine") tr `shouldReturn` Just 945
      insertNew (toInt16 "accelerate-fourier-benchmark") 956 tr `shouldReturn` Nothing
      insertNew (toInt16 "ace") 961 tr `shouldReturn` Nothing
      T.lookup (toInt16 "ace") tr `shouldReturn` Just 961
      T.lookup (toInt16 "ac-machine") tr `shouldReturn` Just 945
      closeTrie tr
  it "Prefix test" $
     withTempFile "trie" $ \f -> do
      tr :: (Trie Int16 Int16) <- openFileTrie f Nothing
      insertNew (toInt16 "A") 15 tr `shouldReturn` Nothing
      insertNew (toInt16 "tea") 3 tr `shouldReturn` Nothing
      insertNew (toInt16 "ted") 4 tr `shouldReturn` Nothing
      insertNew (toInt16 "to") 7 tr `shouldReturn` Nothing
      prefix (toInt16 "AB") tr `shouldReturn` []
      prefix (toInt16 "tea") tr `shouldReturn` [(toInt16 "tea",3)]
      prefix (toInt16 "te") tr `shouldReturn` [(toInt16 "tea",3),(toInt16 "ted",4)]
      prefix (toInt16 "t") tr `shouldReturn` [(toInt16 "tea",3),(toInt16 "ted",4),(toInt16 "to",7)]
      prefix [] tr `shouldReturn` [(toInt16 "A",15),(toInt16 "tea",3),(toInt16 "ted",4),(toInt16 "to",7)]
      closeTrie tr
  it "Int64 keys as Word8" $
      withTempFile "trie" $ \f -> do
        tr :: (Trie Word8 Int16) <- openFileTrie f Nothing
        insertNew (toWord8s (100::Int64)) 15 tr `shouldReturn` Nothing
        T.lookup (toWord8s (100::Int64)) tr `shouldReturn` Just 15
        insertNew (toWord8s (1000::Int64)) 25 tr `shouldReturn` Nothing
        T.lookup (toWord8s (1000::Int64)) tr `shouldReturn` Just 25
        closeTrie tr
  it "Int64 keys as Bits" $
      withTempFile "trie" $ \f -> do
        tr :: (Trie Word8 Int16) <- openFileTrie f Nothing
        insertNew (toBits (100::Int64)) 15 tr `shouldReturn` Nothing
        T.lookup (toBits (100::Int64)) tr `shouldReturn` Just 15
        insertNew (toBits (1000::Int64)) 25 tr `shouldReturn` Nothing
        T.lookup (toBits (1000::Int64)) tr `shouldReturn` Just 25
        closeTrie tr
  it "Int64 keys as Word4" $
      withTempFile "trie" $ \f -> do
        tr :: (Trie Word8 Int16) <- openFileTrie f Nothing
        insertNew (toWord4s (100::Int64)) 15 tr `shouldReturn` Nothing
        T.lookup (toWord4s (100::Int64)) tr `shouldReturn` Just 15
        insertNew (toWord4s (1000::Int64)) 25 tr `shouldReturn` Nothing
        T.lookup (toWord4s (1000::Int64)) tr `shouldReturn` Just 25
        closeTrie tr
  it "Uses a free list" $
    withTempFile "trie" $ \f ->
        withTempFile "freelist" $ \fl -> do
            tr :: (Trie Int16 Int16) <- openFileTrie f (Just fl)
            insertNew (toInt16 "A") 15 tr `shouldReturn` Nothing
            insertNew (toInt16 "tea") 3 tr `shouldReturn` Nothing
            T.delete (toInt16 "A") tr `shouldReturn` Just 15
            insertNew (toInt16 "ted") 4 tr `shouldReturn` Nothing
            T.lookup (toInt16 "A") tr `shouldReturn`  Nothing
            T.delete (toInt16 "ted") tr `shouldReturn`  Just 4
            T.lookup (toInt16 "ted") tr `shouldReturn`  Nothing
            T.lookup (toInt16 "tea") tr `shouldReturn`  Just 3
            insertNew (toInt16 "in") 5 tr `shouldReturn` Nothing
            insertNew (toInt16 "inn") 9 tr `shouldReturn` Nothing
            T.delete (toInt16 "inn") tr `shouldReturn`  Just 9
            T.lookup (toInt16 "in") tr `shouldReturn`  Just 5
            closeTrie tr
  it "Works with multiple threads" $
    withTempFile "trie" $ \f -> multiThread f Nothing
  it "Works with multiple threads and free list " $
    withTempFile "trie" $ \f ->
        withTempFile "freelist" $ \fl -> multiThread f (Just fl)

multiThread :: FilePath -> Maybe FilePath -> IO()
multiThread f mfl = do
    mt 50
    mt 10
  where
    mt nb = do
      tr <- openFileTrie f mfl
      asyncs <- forM [1..nb] $ \n-> async $ 25 `timesDo` trieOp tr n
      forM_ asyncs wait
      T.lookup (toInt16 "A") tr `shouldReturn` Just 15
      T.lookup (toInt16 "tea") tr `shouldReturn` Just 3
      T.lookup (toInt16 "ted") tr `shouldReturn` Just 4
      T.lookup (toInt16 "ten") tr `shouldReturn`Just 12
      T.lookup (toInt16 "to") tr `shouldReturn` Just 7
      T.lookup (toInt16 "in") tr `shouldReturn` Just 5
      T.lookup (toInt16 "inn") tr `shouldReturn` Just 9
      T.lookup (toInt16 "i") tr `shouldReturn`Just 11
      T.lookup (toInt16 "none") tr `shouldReturn` Nothing
      forM [1..nb] $ \n->
        T.lookup (toInt16 (T.pack $ "nb" ++ show n)) tr `shouldReturn`Just n
      closeTrie tr

trieOp :: Trie Int16 Int16 -> Int16 -> IO ()
trieOp tr n = do
    T.delete (toInt16 "A") tr
    T.delete (toInt16 "inn") tr
    T.delete (toInt16 "tea") tr
    T.delete (toInt16 "i") tr
    T.delete (toInt16 "ted") tr
    T.delete (toInt16 "ten") tr
    T.delete (toInt16 "in") tr
    T.delete (toInt16 "to") tr
    T.delete (toInt16 (T.pack $ "nb" ++ show n)) tr
    insert (toInt16 "A") 15 tr
    insert (toInt16 "tea") 3 tr
    insert (toInt16 "ted") 4 tr
    insert (toInt16 "ten") 12 tr
    insert (toInt16 "to") 7 tr
    insert (toInt16 "in") 5 tr
    insert (toInt16 "inn") 9 tr
    insert (toInt16 (T.pack $ "nb" ++ show n)) n tr
    insert (toInt16 "i") 11 tr
    return ()

-- | Convert a text to an array of int16
toInt16 :: T.Text -> [Int16]
toInt16 = map (fromIntegral . fromEnum) . T.unpack

