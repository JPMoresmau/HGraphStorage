{-# LANGUAGE RecordWildCards, MultiParamTypeClasses, TypeSynonymInstances,OverloadedStrings, FlexibleContexts, ConstraintKinds, ExplicitForAll, ScopedTypeVariables  #-}
-- | Operations on the data files
module Database.Graph.HGraphStorage.FileOps where

import Control.Applicative
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
import Control.Monad (foldM, when, join)
import Control.Monad.Base (MonadBase)
import Control.Exception.Lifted (throwIO)
import Control.Monad.IO.Class (liftIO)
import Database.Graph.HGraphStorage.FreeList

-- | Open all the file handles.
open :: FilePath -> GraphSettings -> IO Handles
open dir gs = do
  createDirectoryIfMissing True dir
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
    liftIO $ do
      hSeek (hPropertyValues hs) AbsoluteSeek 0
      BS.hPut (hPropertyValues hs) t
      return ()
  ots <- readAll hs
  rts <- readAll hs
  mdlWithProps <- foldM addProp defMdl pts 
  mdlWithObjs <- foldM addOType mdlWithProps ots
  foldM addRType mdlWithObjs rts
  where
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

-- | Read a binary with a given ID from the given handle
readGeneric :: (GraphUsableMonad m)
  => (Integral a, Binary b) => Handle -> Int64 -> a -> m b
readGeneric h sz a =
  liftIO $ do
    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
    decode <$> BS.hGet h (fromIntegral sz)

-- | Read all binary objects from a given handle, generating their IDs from their offset
readAll :: (GraphUsableMonad m,GraphIdSerializable a b) => Handles -> m [(a,b)]
readAll hs = foldAll hs (\a b->return $ b:a) []

-- | Read all binary objects from a given handle, generating their IDs from their offset
foldAllGeneric :: (GraphUsableMonad m)
  => (Integral a, Eq b, Binary b, Default b) => Handle -> Int64 
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
writeProperty hs ptid nextid v = do
  let h = hPropertyValues hs
  liftIO $ hSeek h SeekFromEnd 0
  off <- liftIO $ fromIntegral <$> hTell h
  let bs = toBin v
  liftIO $ BS.hPut h bs
  write hs Nothing $ Property ptid nextid off (BS.length bs) 
  where
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
readPropertyValue hs dt off len = do
  let h = hPropertyValues hs
  liftIO $ hSeek h AbsoluteSeek (fromIntegral off)
  liftIO $ toValue dt <$> BS.hGet h (fromIntegral len)
  where 
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
  write hs = writeGeneric (hObjects hs) (Just $ hObjectFree hs) objectSize
  readOne hs  = readGeneric (hObjects hs) objectSize
  foldAll hs = foldAllGeneric(hObjects hs) objectSize

-- | Serialization methods for RelationID + Relation
instance GraphIdSerializable RelationID Relation where
  write hs = writeGeneric (hRelations hs) (Just $ hRelationFree hs)  relationSize
  readOne hs  = readGeneric (hRelations hs) relationSize
  foldAll hs = foldAllGeneric(hRelations hs) relationSize
  
-- | Serialization methods for PropertyID + Property
instance GraphIdSerializable PropertyID Property where
  write hs = writeGeneric (hProperties hs) (Just $ hPropertyFree hs)  propertySize
  readOne hs  = readGeneric (hProperties hs) propertySize
  foldAll hs = foldAllGeneric(hProperties hs) propertySize

-- | Serialization methods for PropertyTypeID + PropertyType  
instance GraphIdSerializable PropertyTypeID PropertyType where
  write hs = writeGeneric (hPropertyTypes hs) Nothing propertyTypeSize
  readOne hs  = readGeneric (hPropertyTypes hs) propertyTypeSize
  foldAll hs = foldAllGeneric(hPropertyTypes hs) propertyTypeSize

-- | Serialization methods for ObjectTypeID + ObjectType  
instance GraphIdSerializable ObjectTypeID ObjectType where
  write hs = writeGeneric (hObjectTypes hs) Nothing objectTypeSize
  readOne hs  = readGeneric (hObjectTypes hs) objectTypeSize
  foldAll hs = foldAllGeneric(hObjectTypes hs) objectTypeSize

-- | Serialization methods for RelationTypeID + RelationType  
instance GraphIdSerializable RelationTypeID RelationType where
  write hs = writeGeneric (hRelationTypes hs) Nothing relationTypeSize
  readOne hs  = readGeneric (hRelationTypes hs) relationTypeSize
  foldAll hs = foldAllGeneric(hRelationTypes hs) relationTypeSize
 