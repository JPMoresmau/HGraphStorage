{-# LANGUAGE ScopedTypeVariables,DeriveGeneric #-}
-- | Transactional trie
-- The trie uses the entity key + the transaction id that created the record as the key.
-- Values are the transaction id that deleted the record and the entity value
module Database.LowLevelDB.TxTrie
  ( txLookup
  , txInsert
  , txDelete
  , txPrefix
  , TxTrie
  , openTxTrie
  , TxValue
) where

import Database.LowLevelDB.Conversions
import Database.LowLevelDB.MVCC
import Database.LowLevelDB.Trie
import Data.Word
import Data.Maybe
import Data.Default
import Foreign.Storable
import Foreign.Storable.Record  as Store
import Control.Monad.IO.Class
import Data.Typeable
import GHC.Generics (Generic)

-- | the value in the trie keeps the entity value along with the transaction id that deleted the record
data TxValue v = TxValue
    {  txMax :: Word64
    ,   txVal :: v
    } deriving (Eq,Typeable,Generic)

-- | Type of transaction tries, using Word8 as the key component
type TxTrie v = Trie Word8 (TxValue v)

-- | open a transactional trie on a given file with an optional free list file
openTxTrie  :: (Eq v,Storable v,Default v,MonadIO m) => FilePath -> Maybe FilePath -> m (TxTrie v)
openTxTrie file mflf = openFileTrie file mflf

-- | Default instance
instance Default v => Default (TxValue v)where
    def = TxValue def def

-- | Storable dictionary
storeTxValue :: (Storable v) => Store.Dictionary (TxValue v)
storeTxValue = Store.run $
  TxValue
     <$> Store.element txMax
     <*> Store.element txVal


-- | Storable instance
instance (Storable v) => Storable (TxValue v)  where
    sizeOf = Store.sizeOf storeTxValue
    alignment = Store.alignment storeTxValue
    peek = Store.peek storeTxValue
    poke = Store.poke storeTxValue

-- | Retrieve a matching item for the given key in the given transaction
txLookup:: (TransactionManager m,TrieConstraint k v m,TxKey k) => Transaction -> TxTrie v -> k -> m (Maybe v)
txLookup tx tr k = getRecords tr k >>= readRecord tx >>= return . fmap rValue

-- | Insert a key and value in the given transaction
txInsert :: (TransactionManager m,TrieConstraint k v m,TxKey k) => Transaction -> TxTrie v -> k -> v -> m Transaction
txInsert tx tr k v = do
    recs <- getRecords tr k
    (tx2,_,mods)<-writeRecord tx recs v
    writeRecords tr k mods
    return tx2

-- | Delete a record in the given transaction
txDelete :: (TransactionManager m,TrieConstraint k v m,TxKey k) => Transaction -> TxTrie v -> k -> m Transaction
txDelete tx tr k = do
    recs <- getRecords tr k
    (tx2,_,mods)<-deleteRecord tx recs
    writeRecords tr k mods
    return tx2

-- | Search for a given prefix in the given transaction
txPrefix :: forall k v m.(TransactionManager m,TrieConstraint k v m,TxKey k) => Transaction -> TxTrie v -> [Word8] -> m [(k,v)]
txPrefix tx tr prf = do
    keyVals <- prefixF isVisible prf tr
    let recIDs = map toRecordID keyVals
    return $ map (\(i,r)-> (i,rValue r)) recIDs
  where
    isVisible :: [Word8] -> (TxValue v) -> m Bool
    isVisible k v = do
        let (_::k,r)=toRecordID (k,v)
        ok <- readRecord tx [r]
        return $ isJust ok

-- | Get all records for a given key
getRecords :: (TrieConstraint k v m,TxKey k) => TxTrie v -> k -> m [Record v]
getRecords tr k =  do
    let prf = keyPrefix k
    keyVals <- prefix prf tr
    return $ map (toRecord k) keyVals

-- | Write all given records to the trie
writeRecords ::  (TrieConstraint k v m,TxKey k) => TxTrie v  -> k -> [Record v] -> m ()
writeRecords tr k rs = mapM_ writeR rs
  where
    writeR (Record mn mx v) = insert (toKey (k,mn)) (TxValue mx v) tr

-- | Transform key component list (containing the creation tx) and tx value (containing the deletion tx) into record
toRecord :: forall k v.(TxKey k) => k -> ([Word8],TxValue v) -> Record v
toRecord _ r  = snd $ (toRecordID r :: (k,Record v))

-- | Transform key component list (containing the creation tx) and tx value (containing the deletion tx) into record and entity id
toRecordID :: forall k v.(TxKey k) => ([Word8],TxValue v) -> (k,Record v)
toRecordID (ks,TxValue mx v) = let
    (i,mn) = from ks
    in (i,Record mn mx v)
  where
    from :: [Word8] -> (k,Word64)
    from = fromKey
