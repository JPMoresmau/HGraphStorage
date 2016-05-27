{-# LANGUAGE ScopedTypeVariables #-}
-- | MVCC tests
module Database.LowLevelDB.MVCCSpec where

import Database.LowLevelDB.MVCC

import Test.Hspec
import Control.Monad
import Control.Monad.Trans.Class

spec :: Spec
spec = describe "MVCC tests" $ do
  describe "Single and serialized transactions" $ do
      it "Works on single transactions" $
        void $ withTransactions $ do
            tx1 <- newTransaction
            mv0::Maybe(Record String) <- readRecord tx1 []
            lift (mv0 `shouldBe` Nothing)
            (tx1',rs1) <- writeRecord tx1 [] "foo"
            mv1 <- readRecord tx1' rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx1'',rs2) <- deleteRecord tx1' rs1
            lift (rs2 `shouldBe` [])
            commit tx1''
      it "Works on serialized committed transactions" $
        void $ withTransactions $ do
            tx1 <- newTransaction
            (tx1',rs1) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTransaction
            mv1 <- readRecord tx2 rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx2',rs2) <- deleteRecord tx2 rs1
            commit tx2'
            tx3 <- newTransaction
            mv2 <- readRecord tx3 rs2
            lift (mv2 `shouldBe` Nothing)
            commit tx3
      it "Works on update on serialized committed transactions" $
        void $ withTransactions $ do
            tx1 <- newTransaction
            (tx1',rs1) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTransaction
            mv1 <- readRecord tx2 rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx2',rs2) <- writeRecord tx2 rs1 "bar"
            commit tx2'
            tx3 <- newTransaction
            mv2 <- readRecord tx3 rs2
            lift (rValue <$> mv2 `shouldBe` Just "bar")
            (tx3',rs3) <- deleteRecord tx2 rs2
            commit tx3'
            tx4 <- newTransaction
            mv3 <- readRecord tx4 rs3
            lift (rValue <$> mv3 `shouldBe` Nothing)
            commit tx4
      it "Works on creation from serialized aborted transactions" $
        void $ withTransactions $ do
            tx1 <- newTransaction
            (tx1',rs1) <- writeRecord tx1 [] "foo"
            rollback tx1'
            tx2 <- newTransaction
            mv1 <- readRecord tx2 rs1
            lift (mv1 `shouldBe` Nothing)
      it "Works on deletion from serialized aborted transactions" $
        void $ withTransactions $ do
            tx1 <- newTransaction
            (tx1',rs1) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTransaction
            mv1 <- readRecord tx2 rs1
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            (tx2',rs2) <- deleteRecord tx2 rs1
            rollback tx2'
            tx3 <- newTransaction
            mv2 <- readRecord tx3 rs2
            lift (rValue <$> mv2 `shouldBe` Just "foo")
            commit tx3
