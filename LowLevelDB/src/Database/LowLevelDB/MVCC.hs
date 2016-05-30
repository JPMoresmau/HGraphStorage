{-# LANGUAGE DeriveFunctor,DeriveGeneric,FlexibleContexts,GeneralizedNewtypeDeriving,MultiParamTypeClasses,RecordWildCards #-}
-- | MVCC implementation
--
-- <https://en.wikipedia.org/wiki/Multiversion_concurrency_control>
module Database.LowLevelDB.MVCC
    ( TransactionManager(..)
    , MemoryTransactionManager(..)
    , withTransactions
    , newTransaction
    , commit
    , rollback
    , Record(..)
    , readRecord
    , writeRecord
    , deleteRecord
    ) where

import Control.Applicative
import Data.Default
import Data.Maybe
import Data.Typeable
import GHC.Generics (Generic)

import Data.Int
import qualified Data.Set as DS
import qualified Data.Map as DM

import Control.Monad.State.Lazy

data TransactionStatus = Started | Committed | Aborted
    deriving (Show,Read,Eq,Ord,Enum,Bounded,Typeable,Generic)

data Transaction = Transaction
    {txId :: Int64
    , txStatus :: TransactionStatus
    , txCommittedID :: Int64
    , txCreated :: Int
    , txDeleted :: Int
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

data MemoryTransactionManager = MemoryTransactionManager
    {txmActive :: DS.Set Int64
    , txmAll :: DM.Map Int64 Transaction
    , txmLast :: Int64
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

class (Monad m) => TransactionManager m where
    newTx :: m Transaction
    lastID :: m Int64
    updateTx :: Transaction -> m ()
    deleteTx :: Transaction -> m ()
    getTx :: Int64 -> m (Maybe Transaction)

--instance TransactionManager MemoryTransactionManager where


data Record r = Record
    { rMin :: Int64
    , rMax :: Int64
    , rValue :: r
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Default MemoryTransactionManager where
    def = MemoryTransactionManager DS.empty DM.empty 0


-- | Our monad transformer.
newtype MVCCStateT m a = Gs { uns :: StateT MemoryTransactionManager m a }
    deriving ( Functor, Applicative, Alternative, Monad
             , MonadFix, MonadPlus, MonadTrans )

instance (Monad m) => MonadState MemoryTransactionManager (MVCCStateT m) where
    get = Gs get
    put = Gs . put
    state = Gs . state

instance (Monad m) =>TransactionManager (MVCCStateT m) where
    newTx = newTransaction
    lastID = do
        tm <- get
        return $ txmLast tm
    updateTx tx = do
        tm <- get
        put tm{txmAll=DM.insert (txId tx) tx (txmAll tm)}
    getTx txId = do
        tm <- get
        return  $ DM.lookup txId $ txmAll tm
    deleteTx tx= do
        tm <- get
        let tm2 = if hasWritten tx
                              then tm {txmAll = DM.insert (txId tx) tx (txmAll tm)}
                              else tm {txmAll = DM.delete (txId tx) (txmAll tm)}
        put tm2{txmActive=DS.delete (txId tx) (txmActive tm2)}

withTransactions :: MVCCStateT m a -> m (a,MemoryTransactionManager)
withTransactions st = runStateT (uns st) def

newTransaction :: (Monad m)=>MVCCStateT m Transaction
newTransaction  = do
    tm <- get
    let next = txmLast tm + 1
        tx = Transaction next Started def 0 0
    put tm
        { txmActive=DS.insert next (txmActive tm)
        , txmAll = DM.insert next tx (txmAll tm)
        , txmLast=next}
    return tx

commit :: (TransactionManager m) => Transaction -> m ()
commit = closeTx Committed

rollback :: (TransactionManager m) => Transaction -> m ()
rollback = closeTx Aborted

updateActiveTransaction :: (TransactionManager m)=> Transaction -> m ()
updateActiveTransaction tx = updateTx tx

writeRecord :: (TransactionManager m) => Transaction -> [Record r] -> r -> m (Transaction,[Record r])
writeRecord Transaction{..} _ _ | txStatus /= Started = error "Transaction has commited or aborted"
writeRecord tx@Transaction{..} [] r = do
    let tx2=tx{txCreated=txCreated+1}
    updateActiveTransaction tx2
    return (tx2,[Record txId def r])
writeRecord tx records r = do
    vis <- mapM (\r2->do
        v<-isVisible tx r2
        return (r2,v))records
    let (tx2,rs) = foldr write (tx,[]) vis
    updateActiveTransaction tx2
    return (tx2,rs)
    where
        write (rec,vis) (tx1,recs) | vis==False = (tx1,rec:recs)
        write (rec,_) (tx1@Transaction{..},recs) =
            let mn= rMin rec
                mine = txId == mn
                newRecs = if mine then [rec{rValue=r,rMax=def}] else [Record txId def r,rec{rMax=txId}]
            in (tx1{txCreated=txCreated+1,txDeleted=txDeleted+1},newRecs++recs)

deleteRecord :: (TransactionManager m) => Transaction -> [Record r] -> m (Transaction,[Record r])
deleteRecord Transaction{..} _  | txStatus /= Started = error "Transaction has commited or aborted"
deleteRecord tx [] = return (tx,[])
deleteRecord tx records = do
    vis <- mapM (\r->do
        v<-isVisible tx r
        return (r,v))records
    let (tx2,rs) = foldr write (tx,[]) vis
    updateActiveTransaction tx2
    return (tx2,rs)
    where
        write (rec,vis) (tx1,recs) | vis == False = (tx1,rec:recs)
        write (rec,_) (tx1@Transaction{..},recs) =
            let mn= rMin rec
                mine = txId == mn
                newRecs = if mine then [] else [rec{rMax=txId}]
            in (tx1{txDeleted=txDeleted+1},newRecs++recs)

readRecord :: (TransactionManager m) => Transaction -> [Record r] -> m (Maybe (Record r))
readRecord _ [] = return Nothing
readRecord tx records = do
    vis<-filterM (isVisible tx) records -- visible records
    return $ listToMaybe vis

isCommittedBefore :: (TransactionManager m) => Transaction -> Int64 -> m Bool
isCommittedBefore _ tId | tId==def  = return False
isCommittedBefore tx tId = do
    mtx <- getTx  tId
    return $ maybe False
        (\t->txStatus t==Committed && (txCommittedID t)<(txId tx))
        mtx

-- |
-- <http://momjian.us/main/writings/pgsql/mvcc.pdf>
-- <http://momjian.us/main/writings/pgsql/internalpics.pdf>
isVisible ::  (TransactionManager m) => Transaction -> Record r -> m Bool
isVisible tx@Transaction{..} Record{..}= do
    minCB <- isCommittedBefore tx rMin -- inserted by a commited transaction
    maxCB <-isCommittedBefore tx rMax -- deleted by a committed transaction
    return $ (rMin == txId -- inserted by current transaction
                        && (rMax==def)) -- not deleted
                    || (minCB -- inserted by a commited transaction
                        && (rMax == def -- not deleted
                            || not maxCB)) -- deleted by a transaction not committed

closeTx :: (TransactionManager m) => TransactionStatus -> Transaction -> m ()
closeTx ts tx = do
    lastId <-  lastID
    let tx2 = if Committed == ts
                    then tx{txStatus=ts,txCommittedID=lastId}
                    else tx{txStatus=ts}
    deleteTx tx2


hasWritten :: Transaction -> Bool
hasWritten Transaction{..} = txCreated>0 || txDeleted>0
