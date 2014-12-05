{-# LANGUAGE RecordWildCards, MultiParamTypeClasses, TypeSynonymInstances,OverloadedStrings, FlexibleContexts  #-}
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
import Data.ByteString.Lazy (toStrict)
import Data.Text.Encoding (decodeUtf8)
import Control.Monad (foldM, when)
import Control.Monad.Base (MonadBase)
import Control.Exception.Lifted (throwIO)


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


readModel :: Handles -> IO Model
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
    hSeek (hPropertyValues hs) AbsoluteSeek 0
    BS.hPut (hPropertyValues hs) t
    return ()
  mdlWithProps <- foldM addProp defMdl pts 
  return mdlWithProps
  where
    addProp mdl (ptId,pt) = do
      pvs <- readProperty hs mdl namePropertyID (ptFirstProperty pt)
      case pvs of
        [PVText name] -> return mdl {mPropertyTypes = addToLookup ptId (name,dataType $ ptDataType pt) $ mPropertyTypes mdl}
        [] -> throwIO $ NoNameProperty ptId
        _ -> throwIO $ MultipleNameProperty ptId

writeGeneric :: (Integral a, Binary b) => Handle -> Int64 -> Maybe a -> b -> IO a
writeGeneric h sz (Just a) b = do
  hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
  BS.hPut h $ encode b 
  return a
writeGeneric h sz Nothing b = do
  hSeek h SeekFromEnd 0
  allsz <- hTell h
  let a=div allsz $ toInteger sz 
  BS.hPut h $ encode b 
  return $ fromInteger a + 1

readGeneric :: (Integral a, Binary b) => Handle -> Int64 -> a -> IO b
readGeneric h sz a = do
  hSeek h AbsoluteSeek (toInteger (a - 1) * toInteger sz)
  decode <$> BS.hGet h (fromIntegral sz)

readAllGeneric :: (Integral a, Eq b, Binary b, Default b) => Handle -> Int64 -> IO [(a,b)]
readAllGeneric h sz = go (fromIntegral sz) 0 []
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


readProperty :: Handles -> Model -> PropertyTypeID -> PropertyID -> IO [PropertyValue]
readProperty _ _ _ 0  = return []
readProperty hs mdl ptid pid = do
  p <- readOne hs pid
  vs <- if ptid == 0 || pType p == ptid 
          then
            do 
               let m = DM.lookup (pType p) $ toName $ mPropertyTypes mdl
               (_, dt) <- throwIfNothing (UnknownPropertyType (pType p)) m 
               val <- readPropertyValue hs dt (pOffset p) (pLength p)
               return [val]
          else return []
  let p2 = pNext p
  (vs ++) <$> readProperty hs mdl ptid p2 




throwIfNothing ::   (MonadBase IO m,
                     Exception e) =>
                    e -> Maybe a -> m a
throwIfNothing e Nothing = throwIO e
throwIfNothing _ (Just a) = return a


readPropertyValue :: Handles -> DataType -> PropertyValueOffset -> PropertyValueLength -> IO PropertyValue
readPropertyValue hs ptid off len = do
  let h = hPropertyValues hs
  hSeek h AbsoluteSeek (fromIntegral off)
  toValue ptid <$> BS.hGet h (fromIntegral len)
  where 
    toValue DTBinary  = PVBinary
    toValue DTText    = PVText . decodeUtf8 . toStrict
    toValue DTInteger = PVInteger . decode

class (Integral a, Binary b) => GraphIdSerializable a b where
  write   :: Handles -> Maybe a -> b -> IO a
  readOne :: Handles -> a -> IO b
  readAll :: Handles -> IO [(a,b)]

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
 