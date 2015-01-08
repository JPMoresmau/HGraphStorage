{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, ScopedTypeVariables, ConstraintKinds, FlexibleContexts #-}
-- | Index on disk
-- <http://sqlity.net/en/2445/b-plus-tree>
-- <http://en.wikipedia.org/wiki/Trie>
module Database.Graph.HGraphStorage.Index 
  ( Trie (..)
  , newTrie
  , newFileTrie
  , insertNew
  , insert
  , Database.Graph.HGraphStorage.Index.lookup
  , delete)
where

import Control.Applicative
import Data.Binary
import Data.Int (Int64)
import System.IO
import Data.Typeable
import GHC.Generics (Generic)
import Database.Graph.HGraphStorage.Types

import Data.Default
import qualified Data.ByteString.Lazy  as BS
import System.FilePath
import System.Directory


-- | Trie on disk
data Trie k v = Trie 
  { trHandle     :: Handle -- ^ The disk Handle
  , trRecordLength :: Int64 -- ^ The length of a record
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


newFileTrie  :: forall k v. (Binary k,Binary v,Default k,Default v) => FilePath -> IO (Trie k v)
newFileTrie file = do
  let dir = takeDirectory file
  createDirectoryIfMissing True dir
  h<- openBinaryFile file ReadWriteMode
  return $ newTrie h 


-- | Create a new trie with a given handle
-- The limitations are:
-- Key and Value must have a binary representation of constant length!
newTrie :: forall k v. (Binary k,Binary v,Default k,Default v) => Handle -> Trie k v
newTrie h = Trie h (keyL+valL+pointL*2)
  where 
    keyL   = binLength (def::k)
    valL   = binLength (def::v)
    pointL = binLength (def::Int64)


-- | Insert a value if it does not exist in the tree
-- if it exists, return the old value and does nothing
insertNew :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> v -> Trie k v -> IO (Maybe v)
insertNew key val tr = insertValue key val tr $ \h (off,node) -> do
  let v=tnValue node
  if v /= def
    then return $ Just v
    else do
      hSeek h AbsoluteSeek $ fromIntegral off
      BS.hPut h $ encode (node{tnValue=val})
      return Nothing


-- | Insert a value for a key
-- if the value existed for that key, return the old value
insert :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) => [k] -> v -> Trie k v -> IO (Maybe v)
insert key val tr = insertValue key val tr $ \h (off,node) -> do
    hSeek h AbsoluteSeek $ fromIntegral off
    BS.hPut h $ encode (node{tnValue=val})
    let v=tnValue node
    return $ if v /= def
      then Just v
      else Nothing


-- | Insert a value performing a given action if the key is already present
insertValue :: (Binary k,Eq k,Default k,Binary v,Eq v,Default v) 
  => [k] -> v -> Trie k v 
  -> (Handle -> (Int64,TrieNode k v) ->IO (Maybe v))
  -> IO (Maybe v)
insertValue key val tr onExisting =
  readRecord tr 0 >>= insert' key 
  where 
    h = trHandle tr
    insert' [] _ = return Nothing
    insert' (k:ks) Nothing = do
      hSeek h AbsoluteSeek 0
      let newC = TrieNode k (if null ks then val else def) def def
      BS.hPut h $ encode newC
      insertChild ks (Just (0,newC))
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
              hSeek h SeekFromEnd 0
              allsz <- fromIntegral <$> hTell h
              let newN = TrieNode k (if null ks then val else def) def def
              BS.hPut h $ encode newN
              hSeek h AbsoluteSeek $ fromIntegral off
              BS.hPut h $ encode (node{tnNext=allsz})
              insertChild ks (Just (allsz,newN))
    insertChild [] _      = return Nothing
    insertChild _ Nothing = return Nothing
    insertChild ks@(k':ks') (Just (off,node)) = do
      mc <- readChildRecord tr $ tnChild node
      case mc of
        Just c -> insert' ks $ Just c
        Nothing -> do
          hSeek h SeekFromEnd 0
          allsz <- fromIntegral <$> hTell h
          let newC = TrieNode k' (if null ks' then val else def) def def
          BS.hPut h $ encode newC
          hSeek h AbsoluteSeek $ fromIntegral off
          BS.hPut h $ encode (node{tnChild=allsz})
          insertChild ks' (Just (allsz,newC))

-- | Read a given record
readRecord :: (Binary k,Binary v) => Trie k v -> Int64 -> IO (Maybe (Int64,TrieNode k v))
readRecord tr off = do
    hSeek h AbsoluteSeek $ fromIntegral off
    bs <- BS.hGet h isz
    if BS.null bs 
      then return Nothing
      else return $ Just $ (off,decode bs)
  where 
    h = trHandle tr
    isz = fromIntegral $ trRecordLength tr  


-- | Read a given record whose offset must be greater than 0
readChildRecord :: (Binary k,Binary v) => Trie k v -> Int64 -> IO (Maybe (Int64,TrieNode k v))
readChildRecord _ 0 = return Nothing
readChildRecord tr off = readRecord tr off


-- | Lookup a value from a key
lookup :: (Binary k,Eq k,Binary v,Eq v,Default v) => [k] -> Trie k v -> IO (Maybe v)
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
lookupNode :: (Binary k,Eq k,Binary v,Eq v,Default v) => [k] -> Trie k v -> IO (Maybe (Int64,(TrieNode k v)))
lookupNode key tr = do
  (readRecord tr 0) >>= lookup' key 
  where 
    lookup' [] _ = return Nothing
    lookup' _ Nothing = return Nothing
    lookup' (k:ks) (Just (off,node)) = do
      if k == tnKey node
        then 
          if null ks
            then return $ Just (off,node)
            else (readChildRecord tr $ tnChild node) >>= lookup' ks
        else 
          (readChildRecord tr $ tnNext node) >>= lookup' (k:ks)

          

-- | Delete the value associated with a key
-- This only remove the value from the trienode, it doesn't prune the trie in any way.
delete :: forall k v. (Binary k,Eq k,Binary v,Eq v,Default v) => [k] -> Trie k v-> IO (Maybe v)
delete key tr = do
  mnode <- lookupNode key tr
  case mnode of
    Just (off,node) -> do
      let oldV = tnValue node
      if oldV /= def
        then do
          hSeek h AbsoluteSeek $ fromIntegral off
          let (node'::TrieNode k v) = node{tnValue=def}
          BS.hPut h $ encode node'
          return $ Just oldV 
        else return Nothing
    _ -> return Nothing
  where 
    h = trHandle tr
