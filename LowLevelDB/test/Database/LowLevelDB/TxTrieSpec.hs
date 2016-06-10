{-# LANGUAGE OverloadedStrings,ScopedTypeVariables #-}
-- | Test transactional tries
module Database.LowLevelDB.TxTrieSpec where

import Database.LowLevelDB.Trie as T
import Database.LowLevelDB.TxTrie
import Database.LowLevelDB.MVCC
import Database.LowLevelDB.TestUtils
import Data.Int
import Test.Hspec
import Data.Word
import Control.Monad

import Control.Monad.Trans.Class

spec :: Spec
spec = describe "Transactional Trie tests" $ do
  it "Respects Wikipedia example" $
     withTempFile "tmtrie" $ \f1 -> do
        withTempFile "txtrie" $ \f2 -> do
            tr <- openFileTrie f1 Nothing
            tmf <- newTrieTransactionManager tr
            void $ withPersistentTransactions tmf $ do
                dataTr::TxTrie Word8 <- openTxTrie f2 Nothing
                tx0 <- newTx
                tx0' <- txInsert tx0 dataTr (1::Int64) 0xF
                tx0'' <- txInsert tx0' dataTr (2::Int64) 0xB
                l0<-txLookup tx0'' dataTr (1::Int64)
                lift (l0 `shouldBe` Just 0xF)
                l0'<-txLookup tx0'' dataTr (2::Int64)
                lift (l0' `shouldBe` Just 0xB)
                commit tx0''
                tx1 <- newTx
                tx1'<- txInsert tx1 dataTr (1::Int64) 0x11
                l1<-txLookup tx1' dataTr (1::Int64)
                lift (l1 `shouldBe` Just 0x11)
                commit tx1'
                tx2 <- newTx
                l2<-txLookup tx2 dataTr (1::Int64)
                lift (l2 `shouldBe` Just 0x11)
                l2'<-txLookup tx2 dataTr (2::Int64)
                lift (l2' `shouldBe` Just 0xB)
                tx3 <- newTx
                tx3' <- txDelete tx3 dataTr (2::Int64)
                tx3'' <- txInsert tx3' dataTr (3::Int64) 0x12
                commit tx3''
                l3<-txLookup tx2 dataTr (1::Int64)
                lift (l3 `shouldBe` Just 0x11)
                l3'<-txLookup tx2 dataTr (2::Int64)
                lift (l3' `shouldBe` Just 0xB)
                l3'' <- txLookup tx2 dataTr (3::Int64)
                lift (l3'' `shouldBe` Nothing)
                closeTrie dataTr
            closeTrie tr
