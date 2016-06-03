{-# LANGUAGE DeriveFunctor,DeriveGeneric,FlexibleContexts,GeneralizedNewtypeDeriving,MultiParamTypeClasses,RecordWildCards,ScopedTypeVariables #-}
-- | MVCC implementation
--
-- <https://en.wikipedia.org/wiki/Multiversion_concurrency_control>
module Database.LowLevelDB.MVCC
    ( TransactionManager(..)
    , MemoryTransactionManager(..)
    , withTransactions
    , MVCCStateT
    , TrieTransactionManager(..)
    , newTrieTransactionManager
    , withPersistentTransactions
    , MVCCTrieStateT
    , commit
    , rollback
    , Record(..)
    , Transaction(txId,txStatus)
    , TransactionStatus
    , readRecord
    , writeRecord
    , deleteRecord
    ) where

import Database.LowLevelDB.Conversions
import Database.LowLevelDB.Trie as Trie

import Control.Applicative
import Data.Default
import Data.Maybe
import Data.Typeable
import GHC.Generics (Generic)
import Foreign.Ptr
import Foreign.Storable as FS
import Foreign.Storable.Record  as Store
import Data.Int
import Data.Word
import qualified Data.Set as DS
import qualified Data.Map as DM

import Control.Monad.State.Lazy

-- | Transaction status
data TransactionStatus =
        Idle -- ^ Default status (not started)
    | Started  -- ^ Started!
    | Committed -- ^ Committed
    | Aborted -- ^ A rollback has been called
    deriving (Show,Read,Eq,Ord,Enum,Bounded,Typeable,Generic)

-- | Storable instance
instance Storable TransactionStatus where
    sizeOf _ = 1
    alignment _ = 8
    peek ptr = do
        i::Int8 <- FS.peek $ castPtr ptr
        return $ toEnum $ fromIntegral i
    poke ptr a = FS.poke (castPtr ptr) ((fromIntegral $ fromEnum a)::Int8)

-- | A Transaction
data Transaction = Transaction
    {txId :: Word64 -- ^ The (hopefully unique) id
    , txStatus :: TransactionStatus -- ^ Transaction status
    , txCommittedID :: Word64 -- ^ The ID of the last transaction created when this transaction committed or def if not committed yet
    , txCreated :: Int -- ^ Number of records created
    , txDeleted :: Int -- ^ Number of records deleted
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Default instance
instance Default Transaction where
    def = Transaction def Idle def def def

-- | Storable dictionary
storeTransaction :: Store.Dictionary Transaction
storeTransaction = Store.run $
  Transaction
     <$> Store.element txId
     <*> Store.element txStatus
     <*> Store.element txCommittedID
     <*> Store.element txCreated
     <*> Store.element txDeleted

-- | Storable instance
instance Storable Transaction  where
    sizeOf = Store.sizeOf storeTransaction
    alignment = Store.alignment storeTransaction
    peek = Store.peek storeTransaction
    poke = Store.poke storeTransaction

-- | A transaction manager keeping all its information in memory, for testing
data MemoryTransactionManager = MemoryTransactionManager
    {txmActive :: DS.Set Word64
    , txmAll :: DM.Map Word64 Transaction
    , txmLast :: Word64
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Our Transaction Manager API
class (Monad m) => TransactionManager m where
    newTx :: m Transaction
    lastID :: m Word64
    updateTx :: Transaction -> m ()
    deleteTx :: Transaction -> m ()
    getTx :: Word64 -> m (Maybe Transaction)

-- | A Record in the transaction system. Operations deal with a list of Records as input representing all the records for whatever entity we're dealing with
data Record r = Record
    { rMin :: Word64 -- ^ Transaction that created the record
    , rMax :: Word64 -- ^ Transaction that deleted the record, or def
    , rValue :: r -- ^ The actual value of the record
    } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Default memory transaction manager
instance Default MemoryTransactionManager where
    def = MemoryTransactionManager DS.empty DM.empty 0

-- | Our memory monad transformer, keeping the manager in state
newtype MVCCStateT m a = Gs { uns :: StateT MemoryTransactionManager m a }
    deriving ( Functor, Applicative, Alternative, Monad
             , MonadFix, MonadPlus, MonadTrans,MonadIO )

-- | MonadState instance
instance (Monad m) => MonadState MemoryTransactionManager (MVCCStateT m) where
    get = Gs get
    put = Gs . put
    state = Gs . state

-- | Memory implementation of the TransactionManager API
instance (Monad m) =>TransactionManager (MVCCStateT m) where
    newTx = newTransaction
    lastID = do
        tm <- get
        return $ txmLast tm
    updateTx tx = do
        tm <- get
        put tm{txmAll=DM.insert (txId tx) tx (txmAll tm),txmActive=DS.insert (txId tx) (txmActive tm)}
    getTx txId = do
        tm <- get
        return  $ DM.lookup txId $ txmAll tm
    deleteTx tx= do
        tm <- get
        let tm2 = if hasWritten tx
                              then tm {txmAll = DM.insert (txId tx) tx (txmAll tm)}
                              else tm {txmAll = DM.delete (txId tx) (txmAll tm)}
        put tm2{txmActive=DS.delete (txId tx) (txmActive tm2)}

-- | Run actions with a new memory transaction manager, for testing
withTransactions :: MVCCStateT m a -> m (a,MemoryTransactionManager)
withTransactions st = runStateT (uns st) def

-- | New transaction in memory manager
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

-- | A transaction manager storing the transaction information in a Trie
data TrieTransactionManager = TrieTransactionManager
    {ttxmActive :: DS.Set Word64
    , ttxmTrie :: Trie Word8 Transaction
    , ttxmLast :: Word64
    } deriving (Typeable,Generic)

-- | Create a transaction manager from an existing trie
newTrieTransactionManager :: (MonadIO m)=>Trie Word8 Transaction -> m TrieTransactionManager
newTrieTransactionManager tr = do
    (_,_,m)<-getExtra tr
    return $ TrieTransactionManager def tr m

-- | Our trie based monad transformer, keeping the manager in the state
newtype MVCCTrieStateT m a = TGs { unts :: StateT TrieTransactionManager m a }
    deriving ( Functor, Applicative, Alternative, Monad
             , MonadFix, MonadPlus, MonadTrans,MonadIO )

-- | MonadState instance
instance (Monad m) => MonadState TrieTransactionManager (MVCCTrieStateT m) where
    get = TGs get
    put = TGs . put
    state = TGs . state

-- | Run transactional actions with a trie based manager
withPersistentTransactions :: TrieTransactionManager -> MVCCTrieStateT m a -> m (a,TrieTransactionManager)
withPersistentTransactions m st = runStateT (unts st) m

-- | Trie implementation of the manager API
instance (MonadIO m) =>TransactionManager (MVCCTrieStateT m) where
    newTx = do
        tm <- get
        let next = ttxmLast tm + 1
            tx = Transaction next Started def 0 0
        void $ insert (toWord4s next) tx (ttxmTrie tm)
        setExtra (ttxmTrie tm) (def,def,next)
        put tm
            { ttxmActive=DS.insert next (ttxmActive tm)
            , ttxmLast=next}
        return tx
    lastID = do
        tm <- get
        return $ ttxmLast tm
    updateTx tx = do
        tm <- get
        void $ insert (toWord4s (txId tx)) tx (ttxmTrie tm)
        put tm
            { ttxmActive=DS.insert (txId tx) (ttxmActive tm)
            }
    getTx txId = do
        tm <- get
        Trie.lookup (toWord4s txId) $ ttxmTrie tm
    deleteTx tx= do
        tm <- get
        void $ if hasWritten tx
                              then insert (toWord4s (txId tx)) tx (ttxmTrie tm)
                              else delete (toWord4s (txId tx)) (ttxmTrie tm)
        put tm{ttxmActive=DS.delete (txId tx) (ttxmActive tm)}

-- | Commit the given transaction
commit :: (TransactionManager m) => Transaction -> m ()
commit = closeTx Committed

-- | Rollback the given transaction
rollback :: (TransactionManager m) => Transaction -> m ()
rollback = closeTx Aborted

-- | Write a record to a record list in the given transaction, deleting the visible record if any
writeRecord :: (TransactionManager m) => Transaction -> [Record r] -> r -- ^ the new record
    -> m (Transaction,[Record r],[Record r]) -- ^ Returns the modified transaction, the full list of records, and the modified records
writeRecord Transaction{..} _ _ | txStatus /= Started = error "Transaction has commited or aborted"
writeRecord tx@Transaction{..} [] r = do
    let tx2=tx{txCreated=txCreated+1}
    updateTx tx2
    return (tx2,[Record txId def r],[Record txId def r])
writeRecord tx records r = do
    vis <- mapM (\r2->do
        v<-isVisible tx r2
        return (r2,v))records
    let (tx2,rs,mods) = foldr write (tx,[],[]) vis
    updateTx tx2
    return (tx2,rs,mods)
    where
        write (rec,vis) (tx1,recs,mods) | vis==False = (tx1,rec:recs,rec:mods)
        write (rec,_) (tx1@Transaction{..},recs,mods) =
            let mn= rMin rec
                mine = txId == mn
                newRecs = if mine then [rec{rValue=r,rMax=def}] else [Record txId def r,rec{rMax=txId}]
            in (tx1{txCreated=txCreated+1,txDeleted=txDeleted+1},newRecs++recs,newRecs++mods)

-- | Delete a record from a record list
deleteRecord :: (TransactionManager m) => Transaction -> [Record r]
    -> m (Transaction,[Record r],[Record r])-- ^ Returns the modified transaction, the full list of records, and the modified records
deleteRecord Transaction{..} _  | txStatus /= Started = error "Transaction has commited or aborted"
deleteRecord tx [] = return (tx,[],[])
deleteRecord tx records = do
    vis <- mapM (\r->do
        v<-isVisible tx r
        return (r,v))records
    let (tx2,rs,mods) = foldr write (tx,[],[]) vis
    updateTx tx2
    return (tx2,rs,mods)
    where
        write (rec,vis) (tx1,recs,mods) | vis == False = (tx1,rec:recs,mods)
        write (rec,_) (tx1@Transaction{..},recs,mods) =
            let mn= rMin rec
                mine = txId == mn
                newRecs = if mine then [] else [rec{rMax=txId}]
            in (tx1{txDeleted=txDeleted+1},newRecs++recs,newRecs++mods)

-- | Read the visible record if any for the given transaction
readRecord :: (TransactionManager m) => Transaction -> [Record r] -> m (Maybe (Record r))
readRecord _ [] = return Nothing
readRecord tx records = do
    vis<-filterM (isVisible tx) records -- visible records
    return $ listToMaybe vis

-- | Is the given transaction id correspond to a transaction that was committed before the given transaction
isCommittedBefore :: (TransactionManager m) => Transaction -> Word64 -> m Bool
isCommittedBefore _ tId | tId==def  = return False
isCommittedBefore tx tId = do
    mtx <- getTx  tId
    return $ maybe False
        (\t->txStatus t==Committed && (txCommittedID t)<(txId tx))
        mtx

-- | Is a given record visible in the given transaction
-- <http://momjian.us/main/writings/pgsql/mvcc.pdf>
-- <http://momjian.us/main/writings/pgsql/internalpics.pdf>
isVisible ::  (TransactionManager m) => Transaction -> Record r -> m Bool
isVisible tx@Transaction{..} Record{..}= do
    minCB <- isCommittedBefore tx rMin -- inserted by a commited transaction
    maxCB <-isCommittedBefore tx rMax -- deleted by a committed transaction
    return $ (rMin == txId -- inserted by current transaction
                        && (rMax==def)) -- not deleted
                    || (minCB -- inserted by a commited transaction
                        && rMax /= txId -- not deleted by current transaction
                        && (rMax == def -- not deleted
                            || not maxCB)) -- deleted by a transaction not committed

-- | Close a transaction by setting it to the given status
closeTx :: (TransactionManager m) => TransactionStatus -> Transaction -> m ()
closeTx ts tx | txStatus tx == ts = return () -- nothing to do
closeTx ts tx = do
    lastId <-  lastID
    let tx2 = if Committed == ts
                    then tx{txStatus=ts,txCommittedID=lastId}
                    else tx{txStatus=ts}
    deleteTx tx2

-- | Has the given transaction written anything?
hasWritten :: Transaction -> Bool
hasWritten Transaction{..} = txCreated>0 || txDeleted>0
