{-# LANGUAGE DeriveFunctor,DeriveGeneric,GeneralizedNewtypeDeriving,MultiParamTypeClasses,RecordWildCards #-}
-- | Higher Level API to LowLevelDB :-)
module Database.LowLevelDB
   ( TxManagerOptions(..)
   , withTxManager
   , Transactional(..)
   , withTransaction
   , TxT.TxTrie
   , TxT.openTxTrie
   , TxT.withTxTrie
   , closeTrie
   , TransactionManager()
   ) where

import Database.LowLevelDB.Conversions
import Database.LowLevelDB.Trie
import Database.LowLevelDB.MVCC
import Database.LowLevelDB.TxTrie as TxT

import Control.Monad.IO.Class
import Data.Typeable
import GHC.Generics (Generic)
import Control.Monad.State.Strict
import Control.Applicative

-- | Options for the transaction persistence
data TxManagerOptions = TxManagerOptions
    { txMgrTransactionFile :: FilePath -- ^ Transactions
    ,  txMgrTransactionFreeList :: Maybe FilePath -- ^ Transaction free list (optional)
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Run actions within a transaction manager
withTxManager :: (MonadIO m) => TxManagerOptions -> MVCCTrieStateT m a -> m a
withTxManager TxManagerOptions{..} act= do
    withFileTrie txMgrTransactionFile txMgrTransactionFreeList $ \tr -> do
      tmf <- newTrieTransactionManager tr
      fst <$> withPersistentTransactions tmf act

-- | Transactional API
class (TransactionManager m)=>Transactional m where
    txLookup:: (TrieConstraint k v m,TxKey k) => TxTrie v -> k -> m (Maybe v)
    txInsert :: (TrieConstraint k v m,TxKey k) => TxTrie v -> k -> v -> m ()
    txDelete :: (TrieConstraint k v m,TxKey k) => TxTrie v -> k -> m ()
    txCommit :: m()
    txRollback :: m()

-- | Our transaction state
newtype LowLevelDBTxT m a = TxTs { untxt :: StateT Transaction m a }
    deriving ( Functor, Applicative, Alternative, Monad
             , MonadFix, MonadPlus, MonadTrans, MonadIO )

-- | MonadState instance
instance (Monad m) => MonadState Transaction (LowLevelDBTxT m) where
    get = TxTs get
    put = TxTs . put
    state = TxTs . state

-- | Run actions inside one transaction
withTransaction :: (MonadIO m,TransactionManager m)=> LowLevelDBTxT m a -> m a
withTransaction m = do
    tx <- newTx
    evalStateT (untxt m) tx

instance (TransactionManager m) => TransactionManager (LowLevelDBTxT m) where
    newTx = lift newTx
    lastID = lift lastID
    updateTx = lift . updateTx
    deleteTx = lift . deleteTx
    getTx = lift . getTx

-- | Implementation of the API
instance (TransactionManager m) => Transactional (LowLevelDBTxT m) where
    txLookup t k= do
        tx <- get
        TxT.txLookup tx t k
    txInsert t k v = do
        tx<-get
        tx2 <- TxT.txInsert tx t k v
        put tx2
    txDelete t k = do
        tx<-get
        tx2 <- TxT.txDelete tx t k
        put tx2
    txCommit = do
        tx <- get
        commit tx
    txRollback = do
        tx <- get
        rollback tx
