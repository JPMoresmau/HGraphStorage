{-# LANGUAGE RankNTypes,ScopedTypeVariables,DeriveGeneric,GADTs #-}
-- License     :  AllRightsReserved
--
-- Maintainer  :
-- Stability   :
-- Portability :
--
-- |
-- <http://sqlity.net/en/2445/b-plus-tree>
-- <http://en.wikipedia.org/wiki/Trie>
--
-----------------------------------------------------------------------------

module Database.LowLevelDB.Trie
  ( Trie (..)
  , newTrie
  , newFileTrie
  , insertNew
  , insert
  , Database.LowLevelDB.Trie.lookup
  , prefix
  , delete)
where

import Database.LowLevelDB.MMapHandle
import Data.Int (Int64)
import Data.Typeable
import GHC.Generics (Generic)
import Foreign.Storable as FS
import Foreign.Storable.Record  as Store
import Data.Default
import System.FilePath
import System.Directory
import Data.IORef
import Control.Monad

-- | Trie on disk
data Trie k v = Trie
  { trHandle     :: MMapHandle (TrieNode k v) -- ^ The disk Handle
  , trRecordLength :: Int64 -- ^ The length of a record
  , trMax :: IORef Int64 -- ^ End offset
  }

-- | A Trie Node
data TrieNode k v =  TrieNode
  { tnKey       :: k     -- ^ the key (def for nothing)
  , tnValue     :: v     -- ^ the value (def for nothing)
  , tnNext      :: Int64 -- ^ the offset of next sibling (def for nothing)
  , tnChild     :: Int64 -- ^ the offset of first child (def for nothing)
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Storable dictionary
storeTrieNode :: (Storable k,Storable v) =>Store.Dictionary (TrieNode k v)
storeTrieNode = Store.run $
  TrieNode
     <$> Store.element tnKey
     <*> Store.element tnValue
     <*> Store.element tnNext
     <*> Store.element tnChild

-- | Storable instance
instance (Storable k,Storable v) =>Storable (TrieNode k v) where
    sizeOf = Store.sizeOf storeTrieNode
    alignment = Store.alignment storeTrieNode
    peek = Store.peek storeTrieNode
    poke = Store.poke storeTrieNode



instance (Default k, Default v) => Default (TrieNode k v) where
    def = TrieNode def def def def


-- | Build a file backed trie
newFileTrie  :: forall k v. (Storable k,Storable v,Default k,Default v) => FilePath -> IO (Trie k v)
newFileTrie file = do
  let dir = takeDirectory file
  createDirectoryIfMissing True dir
  h<- openMmap file (0,4096) def
  newTrie h



-- | Create a new trie with a given handle
-- The limitations are:
-- Key element and Value must have a binary representation of constant length!
newTrie :: forall k v. (Storable k,Storable v,Default k,Default v) => MMapHandle (TrieNode k v) -> IO (Trie k v)
newTrie h = do
    let sz = fromIntegral $ FS.sizeOf (undefined::(TrieNode k v))
    tn <- peekMM h 0
    ioMx <- newIORef (max (tnNext tn) sz)
    return $ Trie h sz ioMx


-- | Insert a value if it does not exist in the tree
-- if it exists, return the old value and does nothing
insertNew :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => [k] -> v -> Trie k v -> IO (Maybe v)
insertNew key val tr = insertValue key val tr $ \h (off,node) -> do
  let v=tnValue node
  if v /= def
    then return $ Just v
    else do
      pokeMM h (node{tnValue=val})  $ fromIntegral off
      checkOff tr off
      return Nothing


-- | Insert a value for a key
-- if the value existed for that key, return the old value
insert :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => [k] -> v -> Trie k v -> IO (Maybe v)
insert key val tr = insertValue key val tr $ \h (off,node) -> do
    pokeMM h (node{tnValue=val})  $ fromIntegral off
    checkOff tr off
    let v=tnValue node
    return $ if v /= def
      then Just v
      else Nothing

checkOff :: Trie k v  -> Int64 -> IO()
checkOff tr off = do
    v <- readIORef (trMax tr)
    when (off>v) $ void $ atomicModifyIORef (trMax tr) (\mx->(off+isz,mx))
  where
    isz = fromIntegral $ trRecordLength tr

-- | Insert a value performing a given action if the key is already present
insertValue :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v)
  => [k] -> v -> Trie k v
  -> (MMapHandle (TrieNode k v) -> (Int64,TrieNode k v) ->IO (Maybe v))
  -> IO (Maybe v)
insertValue key val tr onExisting =
  readRecord tr (trRecordLength tr) >>= insert' key
  where
    h = trHandle tr
    insert' [] _ = return Nothing
    insert' (k:ks) Nothing = do
      let newC = TrieNode k (if null ks then val else def) def def
      allsz <- getEnd
      pokeMM h newC (fromIntegral allsz)
      insertChild ks (Just (allsz,newC))
    insert' (k:ks) (Just (off,node)) =
      if k == tnKey node
        then case ks of
          [] -> onExisting h (off,node)
          _ -> insertChild ks (Just (off,node))
        else do
          mn <- readChildRecord tr $ tnNext node
          case mn of
            Just n -> insert' (k:ks) $ Just n
            Nothing -> do
              allsz <- getEnd
              let newN = TrieNode k (if null ks then val else def) def def
              pokeMM h newN (fromIntegral allsz)
              pokeMM h (node{tnNext=allsz}) (fromIntegral off)
              insertChild ks (Just (allsz,newN))
    insertChild [] _      = return Nothing
    insertChild _ Nothing = return Nothing
    insertChild ks@(k':ks') (Just (off,node)) = do
      mc <- readChildRecord tr $ tnChild node
      case mc of
        Just c -> insert' ks $ Just c
        Nothing -> do
          allsz <- getEnd
          let newC = TrieNode k' (if null ks' then val else def) def def
          pokeMM h newC (fromIntegral allsz)
          pokeMM h (node{tnChild=allsz}) (fromIntegral off)
          insertChild ks' (Just (allsz,newC))
    getEnd = do
        mx<-atomicModifyIORef (trMax tr) (\mx->(mx+isz,mx))
        v <- readIORef (trMax tr)
        pokeMM h (TrieNode def def v def) 0
        return mx
    isz = fromIntegral $ trRecordLength tr

-- | Read a given record
readRecord :: (Storable k,Eq k,Storable v,Eq v,Default k,Default v) => Trie k v -> Int64 -> IO (Maybe (Int64,TrieNode k v))
readRecord tr off = do
    tn <- peekMM h (fromIntegral off)
    if tn == def
      then return Nothing
      else return $ Just (off, tn)
  where
    h = trHandle tr


-- | Read a given record whose offset must be greater than 0
readChildRecord :: (Storable k,Storable v,Eq k,Eq v,Default k,Default v) => Trie k v -> Int64 -> IO (Maybe (Int64,TrieNode k v))
readChildRecord _ 0 = return Nothing
readChildRecord tr off = readRecord tr off


-- | Lookup a value from a key
lookup :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => [k] -> Trie k v -> IO (Maybe v)
lookup key tr = do
  mnode <- lookupNode key tr
  return $ case mnode of
    Just (_,node) ->
      let v=tnValue node
      in if v /= def
        then Just v
        else Nothing
    _ -> Nothing


-- | Lookup a node from a Key
lookupNode :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => [k] -> Trie k v -> IO (Maybe (Int64, TrieNode k v))
lookupNode key tr = readRecord tr (trRecordLength tr) >>= lookup' key
  where
    lookup' [] r = return r
    lookup' _ Nothing = return Nothing
    lookup' (k:ks) (Just (off,node)) =
      if k == tnKey node
        then
          if null ks
            then return $ Just (off,node)
            else readChildRecord tr (tnChild node) >>= lookup' ks
        else
          readChildRecord tr (tnNext node) >>= lookup' (k : ks)


-- | Return all key and values for the given prefix which may be null (in which case all mappings are returned).
prefix :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => [k] -> Trie k v -> IO [([k],v)]
prefix key tr = lookupNode key tr >>= collect (null key) key
  where
    collect _ _ Nothing = return []
    collect withNexts k (Just (_,node)) = do
      let k' = tnKey node
      let v = tnValue node
      let nk = if withNexts then k++[k'] else k
      let me = if v == def then [] else [(nk,v)]
      subs <- readChildRecord tr (tnChild node) >>= collect True nk
      nexts <- if withNexts then readChildRecord tr (tnNext node) >>= collect True k else return []
      return $ me ++ subs ++ nexts


-- | Delete the value associated with a key
-- This only remove the value from the trienode, it doesn't prune the trie in any way.
delete :: forall k v. (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => [k] -> Trie k v-> IO (Maybe v)
delete key tr = do
  mnode <- lookupNode key tr
  case mnode of
    Just (off,node) -> do
      let oldV = tnValue node
      if oldV /= def
        then do
          let (node'::TrieNode k v) = node{tnValue=def}
          pokeMM h node' $ fromIntegral off
          return $ Just oldV
        else return Nothing
    _ -> return Nothing
  where
    h = trHandle tr


