{-# LANGUAGE RankNTypes,ScopedTypeVariables,DeriveGeneric #-}
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
  , delete
  , binLength)
where

import Database.LowLevelDB.MMapHandle
import Data.Int (Int64)
import Data.Typeable
import GHC.Generics (Generic)
import Data.Binary
import Data.List (unfoldr)
import Data.Default
import Data.Bits
import qualified Data.ByteString.Lazy  as BS
import System.FilePath
import System.Directory
import Data.IORef
import Control.Monad

-- | Trie on disk
data Trie k v = Trie
  { trHandle     :: MMapHandle Word8 -- ^ The disk Handle
  , trRecordLength :: Int64 -- ^ The length of a record
  , trMax :: IORef Int64 -- ^ End offset
  }

-- | A Trie Node
data TrieNode k v = TrieNode
  { tnKey       :: k     -- ^ the key (def for nothing)
  , tnValue     :: v     -- ^ the value (def for nothing)
  , tnNext      :: Int64 -- ^ the offset of next sibling (def for nothing)
  , tnChild     :: Int64 -- ^ the offset of first child (def for nothing)
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)


-- | Simple binary instance
instance (Binary k, Binary v) => Binary (TrieNode k v)



instance (Default k, Default v) => Default (TrieNode k v) where
    def = TrieNode def def def def


-- | Build a file backed trie
newFileTrie  :: forall k v. (Binary k,Binary v,Default k,Default v) => FilePath -> IO (Trie k v)
newFileTrie file = do
  let dir = takeDirectory file
  createDirectoryIfMissing True dir
  h<- openMmap file (0,4069) def
  newTrie h


unroll :: Int64 -> [Word8]
unroll = unfoldr step
  where
    step 0 = Nothing
    step i = Just (fromIntegral i, i `shiftR` 8)

roll :: [Word8] -> Int64
roll   = foldr unstep 0
  where
    unstep b a = a `shiftL` 8 .|. fromIntegral b

-- | Create a new trie with a given handle
-- The limitations are:
-- Key element and Value must have a binary representation of constant length!
newTrie :: forall k v. (Binary k,Binary v,Default k,Default v) => MMapHandle Word8 -> IO (Trie k v)
newTrie h = do
    mx <- roll <$> peekWord8s h 0 4
    ioMx <- newIORef (max mx 4)
    return $ Trie h (keyL+valL+pointL*2) ioMx
  where
    keyL   = binLength (def::k)
    valL   = binLength (def::v)
    pointL = binLength (def::Int64)

-- | Calculates the length of the binary serialization of the given object
binLength :: (Binary b) => b -> Int64
binLength = BS.length . encode

-- | Insert a value if it does not exist in the tree
-- if it exists, return the old value and does nothing
insertNew :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> v -> Trie k v -> IO (Maybe v)
insertNew key val tr = insertValue key val tr $ \h (off,node) -> do
  let v=tnValue node
  if v /= def
    then return $ Just v
    else do
      pokeMMBSL h (encode (node{tnValue=val}))  $ fromIntegral off
      checkOff tr off
      return Nothing


-- | Insert a value for a key
-- if the value existed for that key, return the old value
insert :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> v -> Trie k v -> IO (Maybe v)
insert key val tr = insertValue key val tr $ \h (off,node) -> do
    pokeMMBSL h (encode (node{tnValue=val}))  $ fromIntegral off
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
insertValue :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v)
  => [k] -> v -> Trie k v
  -> (MMapHandle Word8 -> (Int64,TrieNode k v) ->IO (Maybe v))
  -> IO (Maybe v)
insertValue key val tr onExisting =
  readRecord tr 4 >>= insert' key
  where
    h = trHandle tr
    insert' [] _ = return Nothing
    insert' (k:ks) Nothing = do
      let newC = TrieNode k (if null ks then val else def) def def
      allsz <- getEnd
      pokeMMBSL h (encode newC) (fromIntegral allsz)
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
              pokeMMBSL h (encode newN) (fromIntegral allsz)
              pokeMMBSL h (encode (node{tnNext=allsz})) (fromIntegral off)
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
          pokeMMBSL h (encode newC) (fromIntegral allsz)
          pokeMMBSL h (encode (node{tnChild=allsz})) (fromIntegral off)
          insertChild ks' (Just (allsz,newC))
    getEnd = do
        mx<-atomicModifyIORef (trMax tr) (\mx->(mx+isz,mx))
        v <- readIORef (trMax tr)
        pokeWord8s h (unroll v) 0
        return mx
    isz = fromIntegral $ trRecordLength tr

-- | Read a given record
readRecord :: (Binary k,Eq k,Binary v,Eq v,Default k,Default v) => Trie k v -> Int64 -> IO (Maybe (Int64,TrieNode k v))
readRecord tr off = do
    bs <- peekMMBSL h (fromIntegral off) isz
    let tn = decode bs
    if tn == def
      then return Nothing
      else return $ Just (off, tn)
  where
    h = trHandle tr
    isz = fromIntegral $ trRecordLength tr


-- | Read a given record whose offset must be greater than 0
readChildRecord :: (Binary k,Binary v,Eq k,Eq v,Default k,Default v) => Trie k v -> Int64 -> IO (Maybe (Int64,TrieNode k v))
readChildRecord _ 0 = return Nothing
readChildRecord tr off = readRecord tr off


-- | Lookup a value from a key
lookup :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> Trie k v -> IO (Maybe v)
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
lookupNode :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> Trie k v -> IO (Maybe (Int64, TrieNode k v))
lookupNode key tr = readRecord tr 4 >>= lookup' key
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
prefix :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> Trie k v -> IO [([k],v)]
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
delete :: forall k v. (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> Trie k v-> IO (Maybe v)
delete key tr = do
  mnode <- lookupNode key tr
  case mnode of
    Just (off,node) -> do
      let oldV = tnValue node
      if oldV /= def
        then do
          let (node'::TrieNode k v) = node{tnValue=def}
          pokeMMBSL h (encode node') $ fromIntegral off
          return $ Just oldV
        else return Nothing
    _ -> return Nothing
  where
    h = trHandle tr


