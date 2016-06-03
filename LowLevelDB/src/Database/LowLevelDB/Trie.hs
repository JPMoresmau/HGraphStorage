{-# LANGUAGE RankNTypes,ScopedTypeVariables,DeriveGeneric,GADTs,ConstraintKinds #-}
-- | Trie saved to disk, using mmap
--
-- <http://en.wikipedia.org/wiki/Trie>
module Database.LowLevelDB.Trie
  ( Trie
  , TrieConstraint
  , TrieNode
  , openTrie
  , openFileTrie
  , closeTrie
  , insertNew
  , insert
  , Database.LowLevelDB.Trie.lookup
  , prefix
  , prefixF
  , delete
  , getExtra
  , setExtra)
where

import Database.LowLevelDB.MMapHandle
import Database.LowLevelDB.FreeList
import Data.Word
import Data.Typeable
import GHC.Generics (Generic)
import Foreign.Storable as FS
import Foreign.Storable.Record  as Store
import Data.Default
import System.FilePath
import System.Directory
import Data.IORef
import Control.Monad
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Concurrent.MVar

-- | Trie on disk
data Trie k v = Trie
  { trHandle     :: MMapHandle (TrieNode k v) -- ^ The disk Handle
  , trRecordLength :: Word64 -- ^ The length of a record
  , trMax :: IORef Word64 -- ^ End offset
  , trFreeList :: Maybe (MVar (FreeList Word64)) -- ^ Optional free list to reuse offsets
  }

-- | A Trie Node
data TrieNode k v =  TrieNode
  { tnKey       :: k     -- ^ the key (def for nothing)
  , tnValue     :: v     -- ^ the value (def for nothing)
  , tnNext      :: Word64 -- ^ the offset of next sibling (def for nothing)
  , tnChild     :: Word64 -- ^ the offset of first child (def for nothing)
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


-- | Default instance
instance (Default k, Default v) => Default (TrieNode k v) where
    def = TrieNode def def def def

-- | A synonym for the constraints on the key, value, and the monad we operate in
type TrieConstraint k v m = (Eq k,Storable k,Default k,Eq v,Storable v,Default v,MonadIO m)

-- | Build a file backed trie, with an optional file backed free list
openFileTrie  :: (TrieConstraint k v m) => FilePath -> Maybe FilePath -> m (Trie k v)
openFileTrie file mflf = liftIO $ do
  let dir = takeDirectory file
  createDirectoryIfMissing True dir
  h<- openMmap file (0,4096) def
  mfl <- sequenceA $ fmap (\flf->newFileFreeList flf (1::Word64)) mflf
  openTrie h mfl


-- | Create a new trie with a given handle and an optional free list
openTrie :: forall k v m . (TrieConstraint k v m) => MMapHandle (TrieNode k v) -> Maybe (FreeList Word64) -> m (Trie k v)
openTrie h mfl = liftIO $ do
    let sz = fromIntegral $ FS.sizeOf (undefined::(TrieNode k v))
    tn <- peekMM h 0
    ioMx <- newIORef (max (tnNext tn) sz)
    mmv <- sequenceA $ fmap newMVar mfl
    return $ Trie h sz ioMx mmv

-- | Close the trie
closeTrie :: (MonadIO m)=> Trie k v -> m ()
closeTrie tr = do
    closeMmap $ trHandle tr
    case trFreeList tr of
        Just mv -> liftIO $ withMVar mv $ \fl->void $ closeFreeList fl
        _ -> return ()

-- | Get extra info we can stuff in the trie
getExtra ::  (TrieConstraint k v m)=> Trie k v -> m (k,v,Word64)
getExtra tr = do
    tn <- peekMM (trHandle tr) 0
    return (tnKey tn,tnValue tn,tnChild tn)

-- | Set extra info we can stuff in the trie
setExtra ::  (TrieConstraint k v m)=> Trie k v -> (k,v,Word64) -> m ()
setExtra tr (k,v,c)= do
    tn <- peekMM (trHandle tr) 0
    pokeMM (trHandle tr) (tn{tnKey= k,tnValue=v,tnChild=c}) 0


-- | Insert a value if it does not exist in the tree
-- if it exists, return the old value and does nothing
insertNew :: (TrieConstraint k v m) => [k] -> v -> Trie k v -> m (Maybe v)
insertNew key val tr = liftIO $ insertValue key val tr $ \h (off,node) -> do
  let v=tnValue node
  if v /= def
    then return $ Just v
    else do
      pokeMM h (node{tnValue=val})  $ fromIntegral off
      checkOff tr off
      return Nothing


-- | Insert a value for a key
-- if the value existed for that key, return the old value
insert :: (TrieConstraint k v m) => [k] -> v -> Trie k v -> m (Maybe v)
insert key val tr = liftIO $ insertValue key val tr $ \h (off,node) -> do
    pokeMM h (node{tnValue=val})  $ fromIntegral off
    checkOff tr off
    let v=tnValue node
    return $ if v /= def
      then Just v
      else Nothing

-- | Check if the offset is over the current maximum, and updates the maximum if it is
checkOff :: Trie k v  -> Word64 -> IO()
checkOff tr off = do
    v <- readIORef (trMax tr)
    when (off>v) $ void $ atomicModifyIORef (trMax tr) (\mx->(off+isz,mx))
  where
    isz = fromIntegral $ trRecordLength tr

-- | Run an action to needs the free list if we have one
withFreeList ::  Trie k v -> (Maybe (FreeList Word64) -> IO a) -> IO a
withFreeList tr f =case trFreeList tr of
    (Just mv) -> withMVar mv $ \fl -> f $ Just fl
    Nothing -> f Nothing

-- | Insert a value performing a given action if the key is already present
insertValue :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v)
  => [k] -> v -> Trie k v
  -> (MMapHandle (TrieNode k v) -> (Word64,TrieNode k v) ->IO (Maybe v))
  -> IO (Maybe v)
insertValue key val tr onExisting = withFreeList tr $ \fl ->
  readRecord tr (trRecordLength tr) >>= insert' fl key
  where
    h = trHandle tr
    insert' _ [] _ = return Nothing
    insert' fl (k:ks) Nothing = do
      let newC = TrieNode k (if null ks then val else def) def def
      allsz <- getEmpty tr fl
      pokeMM h newC (fromIntegral allsz)
      insertChild fl ks (Just (allsz,newC))
    insert' fl (k:ks) (Just (off,node)) =
      if k == tnKey node
        then case ks of
          [] -> onExisting h (off,node)
          _ -> insertChild fl ks (Just (off,node))
        else do
          mn <- readChildRecord tr $ tnNext node
          case mn of
            Just n -> insert' fl (k:ks) $ Just n
            Nothing -> do
              allsz <- getEmpty tr fl
              let newN = TrieNode k (if null ks then val else def) def def
              pokeMM h newN (fromIntegral allsz)
              pokeMM h (node{tnNext=allsz}) (fromIntegral off)
              insertChild fl ks (Just (allsz,newN))
    insertChild _ [] _      = return Nothing
    insertChild _ _ Nothing = return Nothing
    insertChild fl ks@(k':ks') (Just (off,node)) = do
      mc <- readChildRecord tr $ tnChild node
      case mc of
        Just c -> insert' fl ks $ Just c
        Nothing -> do
          allsz <- getEmpty tr fl
          let newC = TrieNode k' (if null ks' then val else def) def def
          pokeMM h newC (fromIntegral allsz)
          pokeMM h (node{tnChild=allsz}) (fromIntegral off)
          insertChild fl ks' (Just (allsz,newC))

-- | Read a given record
readRecord :: (Storable k,Eq k,Storable v,Eq v,Default k,Default v) => Trie k v -> Word64 -> IO (Maybe (Word64,TrieNode k v))
readRecord tr off = do
    tn <- peekMM h (fromIntegral off)
    if tn == def
      then return Nothing
      else return $ Just (off, tn)
  where
    h = trHandle tr


-- | Read a given record whose offset must be greater than 0
readChildRecord :: (TrieConstraint k v m) => Trie k v -> Word64 -> m (Maybe (Word64,TrieNode k v))
readChildRecord _ 0 = return Nothing
readChildRecord tr off = liftIO $ readRecord tr off


-- | Lookup a value from a key
lookup :: (TrieConstraint k v m) => [k] -> Trie k v -> m (Maybe v)
lookup key tr = liftIO $ withFreeList tr $ \_ -> do
  mnode <- lookupNode key tr
  return $ case mnode of
    Just (_,node) ->
      let v=tnValue node
      in if v /= def
        then Just v
        else Nothing
    _ -> Nothing


-- | Lookup a node from a Key
lookupNode :: (TrieConstraint k v m) => [k] -> Trie k v -> m (Maybe (Word64, TrieNode k v))
lookupNode key tr = fst <$> (liftIO $ lookupNodes key tr)

-- | Return all key and values for the given prefix which may be null (in which case all mappings are returned).
prefix :: (TrieConstraint k v m) => [k] -> Trie k v -> m [([k],v)]
prefix = prefixF (\_ _ -> return True)

-- | Return all key and values matching the given predicate for the given prefix which may be null (in which case all mappings are returned).
prefixF :: (TrieConstraint k v m) => ([k] -> v -> m Bool) -> [k] -> Trie k v -> m [([k],v)]
prefixF filt key tr = lookupNode key tr >>= collect (null key) key
  where
    collect _ _ Nothing = return []
    collect withNexts k (Just (_,node)) = do
      let k' = tnKey node
      let v = tnValue node
      let nk = if withNexts then k++[k'] else k
      me <- if v == def
                        then return []
                        else do
                            ok <- filt nk v
                            return $ if ok then [(nk,v)] else []
      subs <- readChildRecord tr (tnChild node) >>= collect True nk
      nexts <- if withNexts then readChildRecord tr (tnNext node) >>= collect True k else return []
      return $ me ++ subs ++ nexts


-- | Step in the look
data Step k v = ChildOf (Word64,TrieNode k v) | NextOf (Word64,TrieNode k v)
    deriving (Show)

-- | Lookup a node from a Key, keeping all the steps in the process
lookupNodes :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => [k] -> Trie k v -> IO (Maybe (Word64, TrieNode k v),[Step k v])
lookupNodes key tr = readRecord tr (trRecordLength tr) >>= \m-> lookup' key [] m
  where
    lookup' [] steps r = return (r,steps)
    lookup' _ steps Nothing = return (Nothing,steps)
    lookup' (k:ks) steps (Just (off,node)) =
      if k == tnKey node
        then
          if null ks
            then return (Just (off,node),steps)
            else readChildRecord tr (tnChild node) >>= lookup' ks (ChildOf (off,node):steps)
        else
          readChildRecord tr (tnNext node) >>= lookup' (k : ks) (NextOf (off,node):steps)

-- | Delete the value associated with a key
delete :: forall k v m. (TrieConstraint k v m) => [k] -> Trie k v-> m (Maybe v)
delete key tr = liftIO $  withFreeList tr $ \fl -> do
  (mnode,steps) <- lookupNodes key tr
  case mnode of
    Just (off,node) -> do
      let oldV = tnValue node
      if oldV /= def
        then do
          let (node'::TrieNode k v) = node{tnValue=def}
          pokeMM h node' $ fromIntegral off
          when (tnChild node' == def && off>trRecordLength tr) $
            pruneTree (off,node) steps tr fl
          return $ Just oldV
        else return Nothing
    _ -> return Nothing
  where
    h = trHandle tr

-- | Prune tree on delete
pruneTree :: (Storable k,Storable v,Eq v,Default v)=>(Word64,TrieNode  k v) -> [Step k v] -> Trie k v -> Maybe (FreeList Word64) ->IO ()
pruneTree (off,node) steps tr mfl = case mfl of
    Just fl -> addToFreeList off fl >> processSteps fl node steps
    _ -> return ()
    where
        h = trHandle tr
        processSteps _ _ [] = return ()
        processSteps fl me (ChildOf (offp,nodep):st2)=do
            let myNext=tnNext me
                nodep2 = nodep{tnChild=myNext}
            pokeMM h nodep2 $ fromIntegral offp
            when (myNext==def && tnValue nodep2 == def) (addToFreeList offp fl >> processSteps fl nodep2 st2)
        processSteps _ me (NextOf (offs,nodes):_)=
            pokeMM h (nodes{tnNext=tnNext me}) $ fromIntegral offs

-- | Get an empty offset
getEmpty :: (Storable k,Eq k,Default k,Storable v,Eq v,Default v) => Trie k v -> Maybe (FreeList Word64) -> IO Word64
getEmpty tr mfl = do
    moff <- case mfl of
        Just fl -> getFromFreeList fl
        _ -> return Nothing
    case moff of
        Just off -> return off
        _ -> do
            mx<-atomicModifyIORef (trMax tr) (\mx->(mx+isz,mx))
            v <- readIORef (trMax tr)
            tn <- peekMM (trHandle tr) 0
            pokeMM (trHandle tr) (tn{tnNext= v}) 0
            return mx
    where
        isz = fromIntegral $ trRecordLength tr
