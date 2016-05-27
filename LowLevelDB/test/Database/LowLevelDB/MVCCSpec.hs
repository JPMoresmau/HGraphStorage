{-# LANGUAGE ScopedTypeVariables #-}
-- | MVCC tests
module Database.LowLevelDB.MVCCSpec where

import Database.LowLevelDB.MVCC

import Test.Hspec
import Control.Monad
import Control.Monad.Trans.Class
import qualified Data.Map as DM

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
      it "Works on serialized committed transactions" $ do
        (_,tm)<- withTransactions $ do
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
        DM.size (txmAll tm) `shouldBe` 2
        txmLast tm `shouldBe` 3
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
  describe "Concurrent transactions" $ do
    it "Transaction doesn't see uncommitted data" $ do
        void $ withTransactions $ do
            tx1 <- newTransaction
            (_,rs1) <- writeRecord tx1 [] "foo"
            tx2 <- newTransaction
            mv1 <- readRecord tx2 rs1
            lift (mv1 `shouldBe` Nothing)
    it "Transaction doesn't see data added committed after it started" $ do
        void $ withTransactions $ do
            tx1 <- newTransaction
            (tx1',rs1) <- writeRecord tx1 [] "foo"
            tx2 <- newTransaction
            mv1 <- readRecord tx2 rs1
            lift (mv1 `shouldBe` Nothing)
            commit tx1'
            mv2 <- readRecord tx2 rs1
            lift (mv2 `shouldBe` Nothing)
            rollback tx2
    it "Transaction still sees data deleted and committed after it started" $ do
        void $ withTransactions $ do
            tx1 <- newTransaction
            (tx1',rs1) <- writeRecord tx1 [] "foo"
            commit tx1'
            tx2 <- newTransaction
            (tx2',rs2) <- deleteRecord tx2 rs1
            tx3 <- newTransaction
            mv1 <- readRecord tx3 rs2
            lift (rValue <$> mv1 `shouldBe` Just "foo")
            commit tx2'
            mv1' <- readRecord tx3 rs2
            lift (rValue <$> mv1' `shouldBe` Just "foo")
            rollback tx3
    it "Respects Wikipedia example" $ do
        void $ withTransactions $ do
            tx0 <- newTransaction
            (tx0',rs1) <- writeRecord tx0 [] "Foo"
            (tx0'',rs2) <- writeRecord tx0' [] "Bar"
            commit tx0''
            tx1 <- newTransaction
            (tx1',rs1') <- writeRecord tx1 rs1 "Hello"
            commit tx1'
            tx2 <- newTransaction
            mv1 <- readRecord tx2 rs1'
            lift (rValue <$> mv1 `shouldBe` Just "Hello")
            mv2 <- readRecord tx2 rs2
            lift (rValue <$> mv2 `shouldBe` Just "Bar")
            tx3 <- newTransaction
            (tx3',rs2') <- deleteRecord tx3 rs2
            (tx3'',rs3) <- writeRecord tx3' [] "Foo-Bar"
            commit tx3''
            mv1' <- readRecord tx2 rs1'
            lift (rValue <$> mv1' `shouldBe` Just "Hello")
            mv2' <- readRecord tx2 rs2'
            lift (rValue <$> mv2' `shouldBe` Just "Bar")
            mv3 <- readRecord tx2 rs3
            lift (mv3 `shouldBe` Nothing)
