{-# LANGUAGE RecordWildCards, MultiParamTypeClasses, TypeSynonymInstances,OverloadedStrings, FlexibleContexts, ConstraintKinds  #-}
module Database.Graph.HGraphStorage.FileOps where

import Control.Applicative
import Control.Exception.Base (Exception)

import Data.Binary
import Data.Default
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
import Control.Monad (foldM, when)
import Control.Monad.Base (MonadBase)
import Control.Exception.Lifted (throwIO)
import Control.Monad.IO.Class (liftIO)


open :: FilePath -> IO Handles
open dir = do
  createDirectoryIfMissing True dir
  Handles 
    <$> getHandle objectFile
    <*> getHandle objectTypeFile
    <*> getHandle relationFile
    <*> getHandle relationTypeFile
    <*> getHandle propertyFile
    <*> getHandle propertyTypeFile
    <*> getHandle propertyValuesFile
  where
    getHandle :: FilePath -> IO Handle
    getHandle name = do
      let f = dir </> name
      openBinaryFile f ReadWriteMode


close :: Handles -> IO ()
close Handles{..} = do
  hClose hObjects
  hClose hObjectTypes
  hClose hRelations
  hClose hProperties
  hClose hPropertyTypes
  hClose hPropertyValues


readModel :: (GraphUsableMonad m)
  => Handles -> m Model
readModel hs = do
  let defMdl = def
  -- ots <- readAll hs
  --rts <- readAll hs
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
  mdlWithProps <- foldM addProp defMdl pts 
  return mdlWithProps
  where
    addProp mdl (ptId,pt) = do
      pvs <- readProperties hs mdl namePropertyID (ptFirstProperty pt)
      case pvs of
        [(_,PVText name)] -> return mdl {mPropertyTypes = addToLookup ptId (name,dataType $ ptDataType pt) $ mPropertyTypes mdl}
        [] -> throwIO $ NoNameProperty ptId
        _ -> throwIO $ MultipleNameProperty ptId

writeGeneric :: (GraphUsableMonad m)
  => (Integral a, Binary b) => Handle -> Int64 -> Maybe a -> b -> m a
writeGeneric h sz (Just a) b = 
  liftIO $ do 
    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
    BS.hPut h $ encode b 
    return a
writeGeneric h sz Nothing b = 
  liftIO $ do
    hSeek h SeekFromEnd 0
    allsz <- hTell h
    let a=div allsz $ toInteger sz 
    BS.hPut h $ encode b 
    return $ fromInteger a + 1

readGeneric :: (GraphUsableMonad m)
  => (Integral a, Binary b) => Handle -> Int64 -> a -> m b
readGeneric h sz a = do
  liftIO $ do
    hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
    decode <$> BS.hGet h (fromIntegral sz)

readAllGeneric :: (GraphUsableMonad m)
  => (Integral a, Eq b, Binary b, Default b) => Handle -> Int64 -> m [(a,b)]
readAllGeneric h sz = liftIO $ do
  hSeek h AbsoluteSeek 0
  go (fromIntegral sz) 0 []
  where go isz a l = do
            bs <- BS.hGet h isz
            if BS.null bs
              then return l
              else do
                let b = decode bs
                    i = a + 1
                    l2 = if b == def
                           then l
                           else (i,b):l
                go isz i l2


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

throwIfNothing ::   (MonadBase IO m,
                     Exception e) =>
                    e -> Maybe a -> m a
throwIfNothing e Nothing = throwIO e
throwIfNothing _ (Just a) = return a


readPropertyValue :: (GraphUsableMonad m)
  => Handles -> DataType -> PropertyValueOffset -> PropertyValueLength -> m PropertyValue
readPropertyValue hs ptid off len = do
  let h = hPropertyValues hs
  liftIO $ hSeek h AbsoluteSeek (fromIntegral off)
  liftIO $ toValue ptid <$> BS.hGet h (fromIntegral len)
  where 
    toValue DTBinary  = PVBinary
    toValue DTText    = PVText . decodeUtf8 . toStrict
    toValue DTInteger = PVInteger . decode

class (Integral a, Binary b) => GraphIdSerializable a b where
  write   :: (GraphUsableMonad m) => Handles -> Maybe a -> b -> m a
  readOne :: (GraphUsableMonad m) => Handles -> a -> m b
  readAll :: (GraphUsableMonad m) => Handles -> m [(a,b)]


instance GraphIdSerializable ObjectID Object where
  write hs = writeGeneric (hObjects hs) objectSize
  readOne hs  = readGeneric (hObjects hs) objectSize
  readAll hs = readAllGeneric(hObjects hs) objectSize

instance GraphIdSerializable RelationID Relation where
  write hs = writeGeneric (hRelations hs) relationSize
  readOne hs  = readGeneric (hRelations hs) relationSize
  readAll hs = readAllGeneric(hRelations hs) relationSize
  
instance GraphIdSerializable PropertyID Property where
  write hs = writeGeneric (hProperties hs) propertySize
  readOne hs  = readGeneric (hProperties hs) propertySize
  readAll hs = readAllGeneric(hProperties hs) propertySize
  
instance GraphIdSerializable PropertyTypeID PropertyType where
  write hs = writeGeneric (hPropertyTypes hs) propertyTypeSize
  readOne hs  = readGeneric (hPropertyTypes hs) propertyTypeSize
  readAll hs = readAllGeneric(hPropertyTypes hs) propertyTypeSize
  
instance GraphIdSerializable ObjectTypeID ObjectType where
  write hs = writeGeneric (hObjectTypes hs) objectTypeSize
  readOne hs  = readGeneric (hObjectTypes hs) objectTypeSize
  readAll hs = readAllGeneric(hObjectTypes hs) objectTypeSize
  
instance GraphIdSerializable RelationTypeID RelationType where
  write hs = writeGeneric (hRelationTypes hs) relationTypeSize
  readOne hs  = readGeneric (hRelationTypes hs) relationTypeSize
  readAll hs = readAllGeneric(hRelationTypes hs) relationTypeSize
 