{-# LANGUAGE DeriveFunctor,DeriveGeneric,FlexibleContexts,GeneralizedNewtypeDeriving,MultiParamTypeClasses,RecordWildCards #-}
-- | MVCC implementation
--
-- <https://en.wikipedia.org/wiki/Multiversion_concurrency_control>
module Database.LowLevelDB.MVCC
    ( TransactionManager(..)
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

data TransactionManager = TransactionManager
    {txmActive :: DS.Set Int64
    , txmAll :: DM.Map Int64 Transaction
    , txmLast :: Int64
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

data Record r = Record
    { rMin :: Int64
    , rMax :: Int64
    , rValue :: r
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Default TransactionManager where
    def = TransactionManager DS.empty DM.empty 0


-- | Our monad transformer.
newtype MVCCStateT m a = Gs { uns :: StateT TransactionManager m a }
    deriving ( Functor, Applicative, Alternative, Monad
             , MonadFix, MonadPlus, MonadTrans )

instance (Monad m) => MonadState TransactionManager (MVCCStateT m) where
    get = Gs get
    put = Gs . put
    state = Gs . state

withTransactions :: MVCCStateT m a -> m (a,TransactionManager)
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

commit :: (Monad m) => Transaction -> MVCCStateT m ()
commit = closeTx Committed

rollback :: (Monad m) => Transaction -> MVCCStateT m ()
rollback = closeTx Aborted

updateActiveTransaction :: (Monad m)=> Transaction -> MVCCStateT m ()
updateActiveTransaction tx = do
    tm <- get
    put tm{txmAll=DM.insert (txId tx) tx (txmAll tm)}

writeRecord :: (Monad m) => Transaction -> [Record r] -> r -> MVCCStateT m (Transaction,[Record r])
writeRecord Transaction{..} _ _ | txStatus /= Started = error "Transaction has commited or aborted"
writeRecord tx@Transaction{..} [] r = do
    let tx2=tx{txCreated=txCreated+1}
    updateActiveTransaction tx2
    return (tx2,[Record txId def r])
writeRecord tx records r = do
    tm <- get
    let (tx2,rs) = foldr (write tm) (tx,[]) records
    updateActiveTransaction tx2
    return (tx2,rs)
    where
        write tm rec (tx1,recs) | not (isVisible tx1 tm rec) = (tx1,rec:recs)
        write _ rec (tx1@Transaction{..},recs) =
            let mn= rMin rec
                mine = txId == mn
                newRecs = if mine then [rec{rValue=r,rMax=def}] else [Record txId def r,rec{rMax=txId}]
            in (tx1{txCreated=txCreated+1,txDeleted=txDeleted+1},newRecs++recs)

deleteRecord :: (Monad m) => Transaction -> [Record r] -> MVCCStateT m (Transaction,[Record r])
deleteRecord Transaction{..} _  | txStatus /= Started = error "Transaction has commited or aborted"
deleteRecord tx [] = return (tx,[])
deleteRecord tx records = do
    tm <- get
    let (tx2,rs) = foldr (write tm) (tx,[]) records
    updateActiveTransaction tx2
    return (tx2,rs)
    where
        write tm rec (tx1,recs) | not (isVisible tx1 tm rec) = (tx1,rec:recs)
        write _ rec (tx1@Transaction{..},recs) =
            let mn= rMin rec
                mine = txId == mn
                newRecs = if mine then [] else [rec{rMax=txId}]
            in (tx1{txDeleted=txDeleted+1},newRecs++recs)

readRecord :: (Monad m) => Transaction -> [Record r] -> MVCCStateT m (Maybe (Record r))
readRecord _ [] = return Nothing
readRecord tx records = do
    tm <-get
    return $ listToMaybe
                $ filter (isVisible tx tm) records -- visible records

isCommittedBefore :: Transaction -> Int64 -> TransactionManager -> Bool
isCommittedBefore tx tId TransactionManager{..} =
    maybe False
        (\t->txStatus t==Committed && (txCommittedID t)<(txId tx))
        $ DM.lookup tId txmAll

-- |
-- <http://momjian.us/main/writings/pgsql/mvcc.pdf>
-- <http://momjian.us/main/writings/pgsql/internalpics.pdf>
isVisible :: Transaction -> TransactionManager -> Record r -> Bool
isVisible tx@Transaction{..} tm Record{..}=
    (rMin == txId -- inserted by current transaction
        && (rMax==def)) -- not deleted
    || (isCommittedBefore tx rMin tm -- inserted by a commited transaction
        && (rMax == def -- not deleted
            || not (isCommittedBefore tx rMax tm))) -- deleted by a transaction not committed

closeTx :: (Monad m) => TransactionStatus -> Transaction -> MVCCStateT m ()
closeTx ts tx = do
    tm <- get
    let tx2 = if Committed == ts
                    then tx{txStatus=ts,txCommittedID=txmLast tm}
                    else tx{txStatus=ts}
        tm2 = if hasWritten tx
                      then tm {txmAll = DM.insert (txId tx2) tx2 (txmAll tm)}
                      else tm {txmAll = DM.delete (txId tx) (txmAll tm)}
    put tm2{txmActive=DS.delete (txId tx) (txmActive tm2)}

hasWritten :: Transaction -> Bool
hasWritten Transaction{..} = txCreated>0 || txDeleted>0
