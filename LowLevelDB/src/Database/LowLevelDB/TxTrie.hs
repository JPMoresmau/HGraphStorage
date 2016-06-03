{-# LANGUAGE ScopedTypeVariables #-}
-- | Transactional trie
module Database.LowLevelDB.TxTrie
  ( txLookup
  , txInsert
  , txDelete
  , txPrefix
  , TxTrie
  , openTxTrie
) where

import Database.LowLevelDB.Conversions
import Database.LowLevelDB.MVCC
import Database.LowLevelDB.Trie
import Data.Word
import Data.Bits
import Data.Int
import Data.Maybe
import Data.Default
import Foreign.Storable
import Foreign.Storable.Record  as Store
import Control.Monad.IO.Class

data TxValue v = TxValue
    {  txMax :: Int64
    ,   txVal :: v
    } deriving (Eq)

type TxTrie v = Trie Word8 (TxValue v)

openTxTrie  :: (Eq v,Storable v,Default v,MonadIO m) => FilePath -> Maybe FilePath -> m (TxTrie v)
openTxTrie file mflf = openFileTrie file mflf

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

txLookup:: (TransactionManager m,TrieConstraint k v m,Integral k,FiniteBits k) => Transaction -> TxTrie v -> k -> m (Maybe v)
txLookup tx tr k =getRecords tr k >>= readRecord tx >>= return . fmap rValue

txInsert :: (TransactionManager m,TrieConstraint k v m,Integral k,FiniteBits k) => Transaction -> TxTrie v -> k -> v -> m Transaction
txInsert tx tr k v = do
    recs <- getRecords tr k
    (tx2,_,mods)<-writeRecord tx recs v
    writeRecords tr k mods
    return tx2

txDelete :: (TransactionManager m,TrieConstraint k v m,Integral k,FiniteBits k) => Transaction -> TxTrie v -> k -> m Transaction
txDelete tx tr k = do
    recs <- getRecords tr k
    (tx2,_,mods)<-deleteRecord tx recs
    writeRecords tr k mods
    return tx2

txPrefix :: forall k v m.(TransactionManager m,TrieConstraint k v m,Integral k,FiniteBits k) => Transaction -> TxTrie v -> [Word8] -> m [(k,v)]
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

getRecords :: (TrieConstraint k v m,Integral k,FiniteBits k) => TxTrie v -> k -> m [Record v]
getRecords tr k =  do
    let prf = toWord4s k
    keyVals <- prefix prf tr
    return $ map (toRecord k) keyVals

writeRecords ::  (TrieConstraint k v m,Integral k,FiniteBits k) => TxTrie v  -> k -> [Record v] -> m ()
writeRecords tr k rs = mapM_ writeR rs
  where
    writeR (Record mn mx v) = insert (tupleToWord4s (k,mn)) (TxValue mx v) tr

toRecord :: forall k v.(Integral k,FiniteBits k) => k -> ([Word8],(TxValue v)) -> Record v
toRecord _ r  = snd $ (toRecordID r :: (k,Record v))

toRecordID :: forall k v.(Integral k,FiniteBits k) => ([Word8],(TxValue v)) -> (k,Record v)
toRecordID (ks,TxValue mx v) = let
    (i,mn) = from ks
    in (i,Record mn mx v)
  where
    from :: [Word8] -> (k,Int64)
    from = tupleFromWord4s
