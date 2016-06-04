-- | Test high level API
module Database.LowLevelDBSpec where

import Database.LowLevelDB
import Database.LowLevelDB.TestUtils

import Data.Int
import Control.Monad.IO.Class
import Test.Hspec

spec :: Spec
spec = describe "Transactional API tests" $
  it "Simple example" $
     withTempFile "tmtrie" $ \f1 ->
        withTempFile "txtrie" $ \f2 ->
          withTxManager (TxManagerOptions f1 Nothing) (dbAction f2)

dbAction :: (TransactionManager m) => FilePath -> m()
dbAction f = do
    tr<-openTxTrie f Nothing
    withTransaction $ txAction1 tr
    withTransaction $ txAction2 tr

txAction1 :: (Transactional m) => TxTrie Int64 -> m()
txAction1 tr = do
        txInsert tr (1::Int64) 12
        l <- txLookup tr (1::Int64)
        liftIO (l `shouldBe` Just 12)
        txCommit

txAction2 :: (Transactional m) => TxTrie Int64 -> m()
txAction2 tr = do
        l <- txLookup tr (1::Int64)
        liftIO (l `shouldBe` Just 12)
        txRollback
