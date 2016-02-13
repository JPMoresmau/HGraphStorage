{-# LANGUAGE RecordWildCards, MultiParamTypeClasses, TypeSynonymInstances,OverloadedStrings, FlexibleContexts, ConstraintKinds, ExplicitForAll, ScopedTypeVariables  #-}
-- | Operations on the data files
module Database.Graph.HGraphStorage.FileOps where

import Control.Exception.Base (Exception)

import Data.Binary
import Data.Default
import Data.Traversable
import System.FilePath
import qualified Data.Map as DM


import System.IO
import System.Directory

import Database.Graph.HGraphStorage.Constants
import Database.Graph.HGraphStorage.Types
import qualified Data.ByteString.Lazy  as BS
import Data.Int
import Data.ByteString.Lazy (toStrict, fromStrict)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Control.Monad
import Control.Monad.Base (MonadBase)
import Control.Exception.Lifted (throwIO)
import Control.Monad.IO.Class (liftIO)
import Database.Graph.HGraphStorage.FreeList
import           Database.Graph.HGraphStorage.LowLevel.MMapHandle
import Foreign.Storable

-- | Open all the file handles.
open :: FilePath -> GraphSettings -> IO Handles
open dir gs = do
  createDirectoryIfMissing True dir
  if gsUseMMap gs
    then
      MMHandles
        <$> getMMHandle objectFile
        <*> getFreeList objectFile (def::ObjectID)
        <*> getMMHandle objectTypeFile
        <*> getMMHandle relationFile
        <*> getFreeList relationFile (def::RelationID)
        <*> getMMHandle relationTypeFile
        <*> getMMHandle propertyFile
        <*> getFreeList propertyFile (def::PropertyID)
        <*> getMMHandle propertyTypeFile
        <*> getMMHandle propertyValuesFile
        <*> getMMHandle maxIDsFile
    else
      Handles
        <$> getHandle objectFile
        <*> getFreeList objectFile (def::ObjectID)
        <*> getHandle objectTypeFile
        <*> getHandle relationFile
        <*> getFreeList relationFile (def::RelationID)
        <*> getHandle relationTypeFile
        <*> getHandle propertyFile
        <*> getFreeList propertyFile (def::PropertyID)
        <*> getHandle propertyTypeFile
        <*> getHandle propertyValuesFile
  where
    getHandle :: FilePath -> IO Handle
    getHandle name = do
      let f = dir </> name
      h <- openBinaryFile f ReadWriteMode
      setBufferMode h $ gsMainBuffering gs
      return h
    getMMHandle :: (Default a,Storable a) => FilePath -> IO (MMapHandle a)
    getMMHandle name = openMmap (dir </> name) (0,4096) def
    getFreeList :: (Binary a) => FilePath -> a -> IO (FreeList a)
    getFreeList name d = do
      let f = dir </> freePrefix ++ name
      h<- openBinaryFile f ReadWriteMode
      setBufferMode h $ gsFreeBuffering gs
      initFreeList (fromIntegral $ binLength d) h (do
          ex <- doesFileExist f
          when ex $ removeFile f)


-- | Set the buffer mode on the given handle, if provided.
setBufferMode :: Handle -> Maybe BufferMode -> IO()
setBufferMode _ Nothing = return ()
setBufferMode h (Just bm) = hSetBuffering h bm

-- | Close all the file handles
close :: Handles -> IO ()
close Handles{..} = do
  hClose hObjects
  _  <- closeFreeList hObjectFree
  hClose hObjectTypes
  hClose hRelations
  _ <- closeFreeList hRelationFree
  hClose hRelationTypes
  hClose hProperties
  _ <- closeFreeList hPropertyFree
  hClose hPropertyTypes
  hClose hPropertyValues
close MMHandles{..} = do
  closeMmap mhObjects
  _  <- closeFreeList hObjectFree
  closeMmap mhObjectTypes
  closeMmap mhRelations
  _ <- closeFreeList hRelationFree
  closeMmap mhRelationTypes
  closeMmap mhProperties
  _ <- closeFreeList hPropertyFree
  closeMmap mhPropertyTypes
  closeMmap mhPropertyValues
  closeMmap mhMaxIDs

-- | Read the current model from the handles
-- generate a default model if none present (new db)
readModel :: (GraphUsableMonad m)
  => Handles -> m Model
readModel hs = do
  let defMdl = def
  pts <- readAll hs
  when (null pts) $ do
    let t = "name"
    a <- write hs Nothing (Property namePropertyID 0 0 (BS.length t))
    b <- write hs Nothing (PropertyType (dataTypeID DTText) a)
    when (b /= namePropertyID) $ throwIO $ IncoherentNamePropertyTypeID namePropertyID b
    writeName hs t
  ots <- readAll hs
  rts <- readAll hs
  mdlWithProps <- foldM addProp defMdl pts
  mdlWithObjs <- foldM addOType mdlWithProps ots
  foldM addRType mdlWithObjs rts
  where
    writeName Handles{..} t = liftIO $ do
        hSeek hPropertyValues AbsoluteSeek 0
        BS.hPut hPropertyValues t
    writeName MMHandles{..} t = liftIO $ pokeMMBS mhPropertyValues t 0
    addProp mdl (ptId,pt) = addN mdl (ptFirstProperty pt) ptId $ \name ->
        return mdl{mPropertyTypes = addToLookup ptId (name,dataType $ ptDataType pt) $ mPropertyTypes mdl}

    addOType mdl (otId,ot) = addN mdl (otFirstProperty ot) otId $ \name ->
        return mdl {mObjectTypes = addToLookup otId name $ mObjectTypes mdl}

    addRType mdl (rtId,rt) = addN mdl (rtFirstProperty rt) rtId $ \name ->
        return mdl {mRelationTypes = addToLookup rtId name $ mRelationTypes mdl}

    addN mdl pId tId f = do
      pvs <- readProperties hs mdl namePropertyID pId
      case pvs of
        [(_,PVText name)] -> f name
        [] -> throwIO $ NoNameProperty tId
        _ -> throwIO $ MultipleNameProperty tId

-- | Generic write operation: write the given binary using the given ID and record size
-- if no id, we write at then end
-- otherwise we always ensure that we write at the proper offset, which is why we have fixed length records
writeGeneric :: (GraphUsableMonad m)
  => (Integral a,Binary a,Default a, Binary b) => Handle -> Maybe (FreeList a) -> Int64 -> Maybe a -> b -> m a
writeGeneric h _ sz (Just a) b =
  liftIO $ do
    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
    BS.hPut h $ encode b
    return a
writeGeneric h mf sz Nothing b = do
  mid <- liftIO $ join <$> for mf getFromFreeList
  case mid of
    Just i  -> writeGeneric h mf sz (Just i) b
    Nothing -> liftIO $ do
      hSeek h SeekFromEnd 0
      allsz <- hTell h
      let a=div allsz $ toInteger sz
      BS.hPut h $ encode b
      return $ fromInteger a + 1

-- | Generic write operation: write the given binary using the given ID and record size on the given mmap handle
-- if no id, we write at then end
-- otherwise we always ensure that we write at the proper offset, which is why we have fixed length records
writeGenericMM :: (GraphUsableMonad m)
  => (Integral a, Binary a, Default a, Storable b) => MMapHandle b -> Maybe (FreeList a) -> MMapHandle MaxIDs -> (MMapHandle MaxIDs -> IO a)
  -> Maybe a -> b -> m a
writeGenericMM h _ _ _  (Just a) b =
  liftIO $ do
    pokeMM h b (fromIntegral (a - 1) * sizeOf b)
    return a
writeGenericMM h mf mhids f Nothing b = do
  mid <- liftIO $ join <$> for mf getFromFreeList
  case mid of
    Just i  -> writeGenericMM h  mf mhids f (Just i) b
    Nothing -> do
      a <- liftIO $ f mhids
      writeGenericMM h mf mhids f (Just a) b


-- | Read a binary with a given ID from the given handle
readGeneric :: (GraphUsableMonad m)
  => (Integral a, Binary b) => Handle -> Int64 -> a -> m b
readGeneric h sz a =
  liftIO $ do
    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
    decode <$> BS.hGet h (fromIntegral sz)

-- | Read a binary with a given ID from the given mmap handle
readGenericMM :: (GraphUsableMonad m)
  => (Integral a, Storable b) => MMapHandle b -> b -> a  -> m b
readGenericMM h b a = liftIO $ peekMM h ((fromIntegral a - 1) * sizeOf b)

-- | Read all binary objects from a given handle, generating their IDs from their offset
readAll :: (GraphUsableMonad m,GraphIdSerializable a b) => Handles -> m [(a,b)]
readAll hs = foldAll hs (\a b->return $ b:a) []

-- | Read all binary objects from a given handle, generating their IDs from their offset
foldAllGeneric
  :: (GraphUsableMonad m,Integral a, Eq b, Binary b, Default b)
  => Handle -> Int64
  -> (c -> (a,b) -> m c) -> c -> m c
foldAllGeneric h sz f st = do
  liftIO $ hSeek h AbsoluteSeek 0
  go (fromIntegral sz) 0 st
  where go isz a st2 = do
            bs <- liftIO $ BS.hGet h isz
            if BS.null bs
              then return st2
              else do
                let b = decode bs
                    i = a + 1
                l2 <- if b == def
                         then return st2
                         else f st2 (i,b)
                go isz i l2

-- | Read all binary objects from a given mmap handle, generating their IDs from their offset
foldAllGenericMM
  :: (GraphUsableMonad m,Integral a, Eq b, Storable b, Default b)
  => MMapHandle b -> b
  -> (c -> (a,b) -> m c) -> c -> a -> m c
foldAllGenericMM h d f st maxID =
  go 0 st
  where go a st2 = do
            b <- liftIO $ peekMM h (fromIntegral a*sizeOf d)
            let i = a + 1
            l2 <- if b == def
                     then return st2
                     else f st2 (i,b)
            if i >= maxID
              then return l2
              else go i l2

-- | Read all properties, starting from a given one, with an optional filter on the Property Type
readProperties :: (GraphUsableMonad m)
  => Handles -> Model -> PropertyTypeID -> PropertyID -> m [(Property,PropertyValue)]
readProperties _ _ _ pid | pid == def  = return []
readProperties hs mdl ptid pid = do
  p <- readOne hs pid
  vs <- if ptid == def || pType p == ptid
          then
            do
               let m = DM.lookup (pType p) $ toName $ mPropertyTypes mdl
               (_, dt) <- throwIfNothing (UnknownPropertyType (pType p)) m
               val <- readPropertyValue hs dt (pOffset p) (pLength p)
               return [(p,val)]
          else return []
  let p2 = pNext p
  (vs ++) <$> readProperties hs mdl ptid p2


-- | Write a property, knowing the next one in the chain
writeProperty :: (GraphUsableMonad m)
  => Handles -> PropertyTypeID -> PropertyID -> PropertyValue -> m PropertyID
writeProperty hs@Handles{..} ptid nextid v = do
  let h = hPropertyValues
  liftIO $ hSeek h SeekFromEnd 0
  off <- liftIO $ fromIntegral <$> hTell h
  let bs = toBin v
  liftIO $ BS.hPut h bs
  write hs Nothing $ Property ptid nextid off (BS.length bs)
writeProperty hs@MMHandles{..} ptid nextid v = do
  let bs = toBin v
  off <- liftIO $ do
    off<-nextPropertyValueOffset mhMaxIDs (BS.length bs)
    pokeMMBS mhPropertyValues bs (fromIntegral off)
    return off
  write hs Nothing $ Property ptid nextid off (BS.length bs)

-- | Convert a property value to a bytestring
toBin :: PropertyValue -> BS.ByteString
toBin (PVBinary bs) = bs
toBin (PVText t) = fromStrict $ encodeUtf8 t
toBin (PVInteger i) = encode i

-- | Helper method throwing an exception if we got a Maybe, otherwise return the Just value
throwIfNothing ::   (MonadBase IO m,
                     Exception e) =>
                    e -> Maybe a -> m a
throwIfNothing e Nothing = throwIO e
throwIfNothing _ (Just a) = return a

-- | Read a property value given an offset and length
readPropertyValue :: (GraphUsableMonad m)
  => Handles -> DataType -> PropertyValueOffset -> PropertyValueLength -> m PropertyValue
readPropertyValue Handles{..} dt off len = do
  let h = hPropertyValues
  liftIO $ hSeek h AbsoluteSeek (fromIntegral off)
  liftIO $ toValue dt <$> BS.hGet h (fromIntegral len)
readPropertyValue MMHandles{..} dt off len = do
  let h = mhPropertyValues
  liftIO $ toValue dt <$> peekMMBS h (fromIntegral off) (fromIntegral len)

-- | Convert a bytestring to a propertyvalue
toValue :: DataType -> BS.ByteString -> PropertyValue
toValue DTBinary  = PVBinary
toValue DTText    = PVText . decodeUtf8 . toStrict
toValue DTInteger = PVInteger . decode

-- | A class that defines basic read and write operations for a given ID and binary object
class (Integral a, Binary b) => GraphIdSerializable a b where
  write   :: (GraphUsableMonad m) => Handles -> Maybe a -> b -> m a
  readOne :: (GraphUsableMonad m) => Handles -> a -> m b
  foldAll :: (GraphUsableMonad m) => Handles -> (c -> (a,b) -> m c) -> c -> m c


-- | Serialization methods for ObjectID + Object
instance GraphIdSerializable ObjectID Object where
  write Handles{..} = writeGeneric hObjects (Just hObjectFree) objectSize
  write MMHandles{..} = writeGenericMM mhObjects (Just hObjectFree) mhMaxIDs nextObjectID
  readOne Handles{..}  = readGeneric hObjects objectSize
  readOne MMHandles{..}  = readGenericMM mhObjects def
  foldAll Handles{..} = foldAllGeneric hObjects objectSize
  foldAll MMHandles{..} = ((((liftIO $ lastObjectID mhMaxIDs) >>=) .) .) (foldAllGenericMM mhObjects def)

-- | Serialization methods for RelationID + Relation
instance GraphIdSerializable RelationID Relation where
  write Handles{..} = writeGeneric hRelations (Just hRelationFree) relationSize
  write MMHandles{..} = writeGenericMM mhRelations (Just hRelationFree) mhMaxIDs nextRelationID
  readOne Handles{..}  = readGeneric hRelations relationSize
  readOne MMHandles{..}  = readGenericMM mhRelations def
  foldAll Handles{..} = foldAllGeneric hRelations relationSize
  foldAll MMHandles{..} = ((((liftIO $ lastRelationID mhMaxIDs) >>=) .) .) (foldAllGenericMM mhRelations def)

-- | Serialization methods for PropertyID + Property
instance GraphIdSerializable PropertyID Property where
  write Handles{..} = writeGeneric hProperties (Just hPropertyFree)  propertySize
  write MMHandles{..} = writeGenericMM mhProperties (Just hPropertyFree) mhMaxIDs nextPropertyID
  readOne Handles{..}  = readGeneric hProperties propertySize
  readOne MMHandles{..}  = readGenericMM mhProperties def
  foldAll Handles{..} = foldAllGeneric hProperties propertySize
  foldAll MMHandles{..} = ((((liftIO $ lastPropertyID mhMaxIDs) >>=) .) .) (foldAllGenericMM mhProperties def)


-- | Serialization methods for PropertyTypeID + PropertyType
instance GraphIdSerializable PropertyTypeID PropertyType where
  write Handles{..} = writeGeneric hPropertyTypes Nothing propertyTypeSize
  write MMHandles{..} = writeGenericMM mhPropertyTypes Nothing mhMaxIDs nextPropertyTypeID
  readOne Handles{..}  = readGeneric hPropertyTypes propertyTypeSize
  readOne MMHandles{..}  = readGenericMM mhPropertyTypes def
  foldAll Handles{..} = foldAllGeneric hPropertyTypes propertyTypeSize
  foldAll MMHandles{..} = ((((liftIO $ lastPropertyTypeID mhMaxIDs) >>=) .) .) (foldAllGenericMM mhPropertyTypes def)


-- | Serialization methods for ObjectTypeID + ObjectType
instance GraphIdSerializable ObjectTypeID ObjectType where
  write Handles{..} = writeGeneric hObjectTypes Nothing objectTypeSize
  write MMHandles{..} = writeGenericMM mhObjectTypes Nothing mhMaxIDs nextObjectTypeID
  readOne Handles{..}  = readGeneric hObjectTypes objectTypeSize
  readOne MMHandles{..}  = readGenericMM mhObjectTypes def
  foldAll Handles{..} = foldAllGeneric hObjectTypes objectTypeSize
  foldAll MMHandles{..} = ((((liftIO $ lastObjectTypeID mhMaxIDs) >>=) .) .) (foldAllGenericMM mhObjectTypes def)

-- | Serialization methods for RelationTypeID + RelationType
instance GraphIdSerializable RelationTypeID RelationType where
  write Handles{..} = writeGeneric hRelationTypes Nothing relationTypeSize
  write MMHandles{..} = writeGenericMM mhRelationTypes Nothing mhMaxIDs nextRelationTypeID
  readOne Handles{..}  = readGeneric hRelationTypes relationTypeSize
  readOne MMHandles{..}  = readGenericMM mhRelationTypes def
  foldAll Handles{..} = foldAllGeneric hRelationTypes relationTypeSize
  foldAll MMHandles{..} = ((((liftIO $ lastRelationTypeID mhMaxIDs) >>=) .) .) (foldAllGenericMM mhRelationTypes def)

-- | Read max ids from mmap handle
peekMaxIDs :: MMapHandle MaxIDs -> IO MaxIDs
peekMaxIDs mh = peekMM mh 0

-- | Highest assigned object id
lastObjectID :: MMapHandle MaxIDs -> IO ObjectID
lastObjectID = (miObject <$>) . peekMaxIDs

-- | Generate next object id
nextObjectID :: MMapHandle MaxIDs -> IO ObjectID
nextObjectID mh = do
  mids <- peekMaxIDs mh
  let oid=miObject mids + 1
  pokeMM mh mids{miObject = oid} 0
  return oid

-- | Highest assigned relation id
lastRelationID :: MMapHandle MaxIDs -> IO RelationID
lastRelationID = (miRelation <$>) . peekMaxIDs

-- | Generate next relation id
nextRelationID :: MMapHandle MaxIDs -> IO RelationID
nextRelationID mh = do
  mids <- peekMaxIDs mh
  let oid=miRelation mids + 1
  pokeMM mh mids{miRelation = oid} 0
  return oid

-- | Highest assigned property id
lastPropertyID :: MMapHandle MaxIDs -> IO PropertyID
lastPropertyID = (miProperty <$>) . peekMaxIDs

-- | Generate next property id
nextPropertyID :: MMapHandle MaxIDs -> IO PropertyID
nextPropertyID mh = do
  mids <- peekMaxIDs mh
  let oid=miProperty mids + 1
  pokeMM mh mids{miProperty = oid} 0
  return oid

-- | Highest assigned property type id
lastPropertyTypeID :: MMapHandle MaxIDs -> IO PropertyTypeID
lastPropertyTypeID = (miPropertyType <$>) . peekMaxIDs

-- | Generate next property type id
nextPropertyTypeID :: MMapHandle MaxIDs -> IO PropertyTypeID
nextPropertyTypeID mh = do
  mids <- peekMaxIDs mh
  let oid=miPropertyType mids + 1
  pokeMM mh mids{miPropertyType = oid} 0
  return oid

-- | Highest assigned object type id
lastObjectTypeID :: MMapHandle MaxIDs -> IO ObjectTypeID
lastObjectTypeID = (miObjectType <$>) . peekMaxIDs

-- | Generate next object type id
nextObjectTypeID :: MMapHandle MaxIDs -> IO ObjectTypeID
nextObjectTypeID mh = do
  mids <- peekMaxIDs mh
  let oid=miObjectType mids + 1
  pokeMM mh mids{miObjectType = oid} 0
  return oid

-- | Highest assigned relation type id
lastRelationTypeID :: MMapHandle MaxIDs -> IO RelationTypeID
lastRelationTypeID = (miRelationType <$>) . peekMaxIDs

-- | Generate next relation type id
nextRelationTypeID :: MMapHandle MaxIDs -> IO RelationTypeID
nextRelationTypeID mh = do
  mids <- peekMaxIDs mh
  let oid=miRelationType mids + 1
  pokeMM mh mids{miRelationType = oid} 0
  return oid

-- | Generate next property value offset id
nextPropertyValueOffset :: MMapHandle MaxIDs -> PropertyValueOffset -> IO PropertyValueOffset
nextPropertyValueOffset mh inc = do
  mids <- peekMM mh 0
  let oid=miPropertyOffset mids
  pokeMM mh mids{miPropertyOffset = oid + inc} 0
  return oid
