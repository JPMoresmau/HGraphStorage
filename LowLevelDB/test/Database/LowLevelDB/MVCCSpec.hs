{-# LANGUAGE FlexibleContexts,GADTs,ScopedTypeVariables #-}
-- | MVCC tests
module Database.LowLevelDB.MVCCSpec where

import Database.LowLevelDB.MVCC
import Database.LowLevelDB.TestUtils
import Database.LowLevelDB.Trie

import Test.Hspec
import Control.Monad
import Control.Monad.Trans.Class
import qualified Data.Map as DM
import Data.Default

spec :: Spec
spec = describe "MVCC tests" $ do
    describe "Memory transactions" $ mvccSpec withTransactions (\tm-> do
        DM.size (txmAll tm) `shouldBe` 2
        txmLast tm `shouldBe` 3)
    describe "Persistent transactions" $
            mvccSpec (\act -> do
                withTempFile "trie" $ \f -> do
                    withFileTrie f Nothing $ \tr-> do
                      tmf <- newTrieTransactionManager tr
                      withPersistentTransactions tmf act
                      ) (\tm-> do
                ttxmLast tm `shouldBe` 3)


mvccSpec f check = do
  describe "Single and serialized transactions" $ do
      it "Works on single transactions" $
        void $ f $ do
            tx1 <- newTx
            mv0::Maybe(Record String) <- readRecord tx1 []
            lift (mv0 `shouldBe` Nothing)
            (tx1',rs1,mods1) <- writeRecord tx1 [] "foo"
            lift (mods1 `shouldBe` rs1)
            mv1 <- readRecord tx1' rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx1'',rs2,mods2) <- deleteRecord tx1' rs1
            lift (rs2 `shouldBe` [])
            lift (mods2 `shouldBe` rs2)
            commit tx1''
      it "Works on serialized committed transactions" $ do
        (_,tm)<- f $ do
            tx1 <- newTx
            (tx1',rs1,_) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTx
            mv1 <- readRecord tx2 rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx2',rs2,_) <- deleteRecord tx2 rs1
            commit tx2'
            tx3 <- newTx
            mv2 <- readRecord tx3 rs2
            lift (mv2 `shouldBe` Nothing)
            commit tx3
        check tm
      it "Works on update on serialized committed transactions" $
        void $ f $ do
            tx1 <- newTx
            (tx1',rs1,_) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTx
            mv1 <- readRecord tx2 rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx2',rs2,_) <- writeRecord tx2 rs1 "bar"
            commit tx2'
            tx3 <- newTx
            mv2 <- readRecord tx3 rs2
            lift (rValue <$> mv2 `shouldBe` Just "bar")
            (tx3',rs3,_) <- deleteRecord tx2 rs2
            commit tx3'
            tx4 <- newTx
            mv3 <- readRecord tx4 rs3
            lift (rValue <$> mv3 `shouldBe` Nothing)
            commit tx4
      it "Works on creation from serialized aborted transactions" $
        void $ f $ do
            tx1 <- newTx
            (tx1',rs1,_) <- writeRecord tx1 [] "foo"
            rollback tx1'
            tx2 <- newTx
            mv1 <- readRecord tx2 rs1
            lift (mv1 `shouldBe` Nothing)
      it "Works on deletion from serialized aborted transactions" $
        void $ f $ do
            tx1 <- newTx
            (tx1',rs1,_) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTx
            mv1 <- readRecord tx2 rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx2',rs2,_) <- deleteRecord tx2 rs1
            rollback tx2'
            tx3 <- newTx
            mv2 <- readRecord tx3 rs2
            lift (rValue <$> mv2 `shouldBe` Just "foo")
            commit tx3
  describe "Concurrent transactions" $ do
    it "Transaction doesn't see uncommitted data" $ do
        void $ f $ do
            tx1 <- newTx
            (_,rs1,_) <- writeRecord tx1 [] "foo"
            tx2 <- newTx
            mv1 <- readRecord tx2 rs1
            lift (mv1 `shouldBe` Nothing)
    it "Transaction doesn't see data added committed after it started" $ do
        void $ f $ do
            tx1 <- newTx
            (tx1',rs1,_) <- writeRecord tx1 [] "foo"
            tx2 <- newTx
            mv1 <- readRecord tx2 rs1
            lift (mv1 `shouldBe` Nothing)
            commit tx1'
            mv2 <- readRecord tx2 rs1
            lift (mv2 `shouldBe` Nothing)
            rollback tx2
    it "Transaction still sees data deleted and committed after it started" $ do
        void $ f $ do
            tx1 <- newTx
            (tx1',rs1,_) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTx
            (tx2',rs2,_) <- deleteRecord tx2 rs1
            tx3 <- newTx
            mv1 <- readRecord tx3 rs2
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            commit tx2'
            mv1' <- readRecord tx3 rs2
            lift (rValue <$> mv1' `shouldBe` Just "foo")
            rollback tx3
    it "Respects Wikipedia example" $ do
        void $ f $ do
            tx0 <- newTx
            (tx0',rs1,_) <- writeRecord tx0 [] "Foo"
            (tx0'',rs2,_) <- writeRecord tx0' [] "Bar"
            commit tx0''
            tx1 <- newTx
            (tx1',rs1',mods1') <- writeRecord tx1 rs1 "Hello"
            lift (mods1' `shouldBe` [Record (txId tx1) def "Hello",Record (txId tx0) (txId tx1) "Foo"])
            mv11 <- readRecord tx1' rs1'
            lift (rValue <$> mv11 `shouldBe` Just "Hello")
            mv1m <- readRecord tx1' mods1'
            lift (rValue <$> mv1m `shouldBe` Just "Hello")
            mv1r <- readRecord tx1' $ reverse mods1'
            lift (rValue <$> mv1r `shouldBe` Just "Hello")
            commit tx1'
            tx2 <- newTx
            mv1 <- readRecord tx2 rs1'
            lift (rValue <$> mv1 `shouldBe` Just "Hello")
            mv2 <- readRecord tx2 rs2
            lift (rValue <$> mv2 `shouldBe` Just "Bar")
            tx3 <- newTx
            (tx3',rs2',mods2') <- deleteRecord tx3 rs2
            lift (mods2' `shouldBe` [Record (txId tx0) (txId tx3) "Bar"])
            (tx3'',rs3,_) <- writeRecord tx3' [] "Foo-Bar"
            commit tx3''
            mv1' <- readRecord tx2 rs1'
            lift (rValue <$> mv1' `shouldBe` Just "Hello")
            mv2' <- readRecord tx2 rs2'
            lift (rValue <$> mv2' `shouldBe` Just "Bar")
            mv3 <- readRecord tx2 rs3
            lift (mv3 `shouldBe` Nothing)
