{-# LANGUAGE DeriveGeneric, FlexibleContexts,FlexibleInstances, MultiParamTypeClasses, RecordWildCards, FunctionalDependencies, ScopedTypeVariables,TypeSynonymInstances    #-}
module Database.Graph.TGraph.Types

where

import Database.Graph.TGraph.LowLevel.MMapHandle
import Database.Graph.TGraph.LowLevel.FreeList
import Database.Graph.TGraph.Constants

import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString.Lazy                   as BS
import           Data.ByteString.Lazy.Char8             (pack,unpack)
import           Data.Default
import           Data.Int
import qualified Data.Map                               as DM
import           Data.Monoid
import           Data.TCache
import           Data.TCache.DefaultPersistence
import qualified Data.Text                              as T
import           Data.Text.Encoding
import           Data.Typeable
import           GHC.Generics                           (Generic)
import qualified Foreign.Storable.Record  as Store
import           Foreign.Storable
import Data.IORef
import System.IO.Unsafe

import Debug.Trace

-- | IDs for objects
type ObjectID       = Int64
--  deriving (Show,Read,Eq,Ord,Typeable,Generic,Default,Binary,Num,Storable)

-- instance Default ObjectID where
--   def = ObjectID 0
--
-- instance Binary ObjectID where
--   put (ObjectID i) = put i
--   get = ObjectID <$> get

-- | IDs for types of objects
type ObjectTypeID   = Int16

-- | IDs for relations
type RelationID     = Int64
  --deriving (Show,Read,Eq,Ord,Typeable,Generic,Default,Binary,Num,Storable)

-- instance Default RelationID where
--   def = RelationID 0
--
-- instance Binary RelationID where
--   put (RelationID i) = put i
--   get = RelationID <$> get

-- | IDs for types of relations
type RelationTypeID = Int16

-- | IDs for property values
type PropertyID     = Int32

-- | IDs for types of properties
type PropertyTypeID = Int16

-- | IDs for data types
type DataTypeID     = Int8

-- | Offset of property value on value file
type PropertyValueOffset = Int64

-- | Length of property value on value file
type PropertyValueLength = Int64


class (Default typ,Eq typ,Show typ) => HasID dat typ | dat -> typ, typ -> dat where
  toNumID :: DBRef dat -> typ
  toStringID :: typ -> String

maybeID :: (HasID dat typ) => Maybe (DBRef dat) -> typ
maybeID Nothing = def
maybeID (Just db)= toNumID db

maybeRef :: (HasID dat typ,IResource dat,Typeable dat) => typ -> Maybe (DBRef dat)
maybeRef nb
  | nb == def = Nothing
  | otherwise = Just $ getDBRef $ toStringID nb

-- | Calculates the length of the binary serialization of the given object
binLength :: (Binary b) => b -> Int64
binLength = BS.length . encode


-- | A lookup table allowing two ways lookup
data Lookup a b = Lookup
  { fromName :: DM.Map b a
  , toName   :: DM.Map a b
  } deriving (Show,Read,Eq,Ord,Typeable)

-- | Default instance (empty tables)
instance Default (Lookup a b) where
  def = Lookup DM.empty DM.empty

-- | Add to the lookup maps
addToLookup :: (Ord a, Ord b) => a -> b -> Lookup a b -> Lookup a b
addToLookup a b (Lookup fn tn) = Lookup (DM.insert b a fn) (DM.insert a b tn)


-- | the supported data types for properties
data DataType = DTText | DTInteger | DTBinary
  deriving (Show,Read,Eq,Ord,Bounded,Enum,Typeable)

dataTypeID :: DataType -> DataTypeID
dataTypeID = fromIntegral . fromEnum

dataTypeFromID :: DataTypeID -> DataType
dataTypeFromID = toEnum . fromIntegral


-- | A typed property value
data PropertyValue =
    PVText    T.Text
  | PVInteger Integer
  | PVBinary  BS.ByteString
  deriving (Show,Read,Eq,Ord,Typeable)

-- | Convert a property value to a bytestring
toBin :: PropertyValue -> BS.ByteString
toBin (PVBinary bs) = bs
toBin (PVText t) = BS.fromStrict $ encodeUtf8 t
toBin (PVInteger i) = encode i

-- | Convert a bytestring to a propertyvalue
toValue :: DataType -> BS.ByteString -> PropertyValue
toValue DTBinary  = PVBinary
toValue DTText    = PVText . decodeUtf8 . BS.toStrict
toValue DTInteger = PVInteger . decode

-- | Get the data type for a given value
valueType :: PropertyValue -> DataType
valueType (PVText _) = DTText
valueType (PVInteger _) = DTInteger
valueType (PVBinary _) = DTBinary

data ObjectType = ObjectType
  { otID :: ObjectTypeID
  , otName :: T.Text
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- instance HasID ObjectType ObjectTypeID where
--   toNumID db = OTID $ read $ drop 2 (keyObjDBRef db)
--   toStringID i = "ot" <> show (unOTID i)
--
-- instance Indexable ObjectType where
--   key ot = toStringID (otID ot)
--
-- instance Serializable ObjectType where
--   serialize ObjectType {..} = -- toLazyByteString $ putInt16be otID <> putInt32be (maybeID otFirstProperty)
--     runPut $ do
--       putInt16be (unOTID otID)
--       putInt32be (maybeID otFirstProperty)
--   deserialize = runGet $
--     ObjectType
--       <$> (OTID <$> get)
--       <*> (maybeRef <$> get)

-- | Type of a property as represented in the property type file
data PropertyType = PropertyType
  { ptID            :: PropertyTypeID -- ^ ID
  , ptDataType      :: DataType -- ^ Data type
  , ptName          :: T.Text -- ^ property name
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- instance Indexable PropertyType where
--   key p = toStringID (ptID p)
--
-- instance HasID PropertyType PropertyTypeID where
--   toNumID db = PTID $ read $ drop 2 (keyObjDBRef db)
--   toStringID i = "pt" <> show (unPTID i)
--
-- instance Serializable PropertyType where
--    serialize= pack . show
--    deserialize= read . unpack

-- instance Serializable PropertyType where
--   serialize PropertyType{..} = runPut $ do
--         putInt16be (unPTID ptID)
--         putInt8 (fromIntegral $ fromEnum ptDataType)
--         putInt32be (maybeID ptFirstProperty)
--
--   deserialize = runGet $ PropertyType
--     <$> (PTID <$> get)
--     <*> ((toEnum . fromIntegral) <$> (get :: Get Int8))
--     <*> (maybeRef <$> get)

data RelationType = RelationType
  { rtID :: RelationTypeID
  , rtName :: T.Text
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)



data Model = Model
  { mObjectTypes :: [ObjectType]
  , mPropertyTypes :: [PropertyType]
  , mRelationTypes :: [RelationType]
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Serializable Model where
   serialize= pack . show
   deserialize= read . unpack

instance Indexable Model where
   key = const modelName

instance Default Model where
  def = Model [] [] []

data MaxIDs = MaxIDs
  { miObject :: ObjectID
  , miRelation :: RelationID
  , miPropertyOffset :: PropertyValueOffset
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Serializable MaxIDs where
   serialize= pack . show
   deserialize= read . unpack

instance Indexable MaxIDs where
   key = const maxName

instance Default MaxIDs where
  def = MaxIDs def def def

data MetaData = MetaData
  { mdModel :: DBRef Model
  , mdMaxIDs :: DBRef MaxIDs
  }

metaData :: MetaData
metaData = MetaData (getDBRef modelName) (getDBRef maxName)

-- | An object as represented in the object file
data ObjectRecord = ObjectRecord
  {
    orType          :: ObjectTypeID -- ^ type of object
  , orFirstFrom     :: RelationID   -- ^ first relation starting from the object
  , orFirstTo       :: RelationID   -- ^ first relation arriving at the object
  , orFirstPropertyOffset :: PropertyValueOffset
  , orFirstPropertyLength :: PropertyValueLength -- ^ first property
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Simple binary instance
instance Binary ObjectRecord

-- | Simple default instance
instance Default ObjectRecord where
  def = ObjectRecord 0 0 0 0 0

-- | Storable dictionary
storeObject :: Store.Dictionary ObjectRecord
storeObject = Store.run $
  ObjectRecord
     <$> Store.element orType
     <*> Store.element orFirstFrom
     <*> Store.element orFirstTo
     <*> Store.element orFirstPropertyOffset
     <*> Store.element orFirstPropertyLength

-- | Storable instance
instance Storable ObjectRecord where
    sizeOf = Store.sizeOf storeObject
    alignment = Store.alignment storeObject
    peek = Store.peek storeObject
    poke = Store.poke storeObject

-- | Size of an object record
objectSize :: Int64
objectSize = binLength (def::ObjectRecord)

instance HasID Object (ObjectID,ObjectTypeID) where
  toNumID db = read $ drop 1 (keyObjDBRef db)
  toStringID i = "o" <> show i

data Object = Object
  { oID            :: ObjectID
  , oType          :: ObjectTypeID -- ^ type of object
  , oFirstFrom     :: Maybe (DBRef Relation)   -- ^ first relation starting from the object
  , oFirstTo       :: Maybe (DBRef Relation)   -- ^ first relation arriving at the object
  , oFirstProperty :: Maybe (DBRef Property) -- ^ first property
 }

instance IResource Object where
 keyResource o = toStringID (oID o, oType o)
 writeResource Object{..} = do
   hs <- readIORef handleRef
   let (off,len) = maybeID oFirstProperty
       obr = ObjectRecord oType (maybeID oFirstFrom) (maybeID oFirstTo) off len
       h = mhObjects hs
   pokeMM h obr (fromIntegral (oID - 1) * sizeOf obr)
 readResourceByKey k = do
   hs <- readIORef handleRef
   let (oid,_)::(ObjectID,ObjectTypeID) = read $ drop 1 k
       h = mhObjects hs
   obr <- peekMM h (fromIntegral oid)
   if orType obr == def
     then return Nothing
     else return $ Just $ Object oid (orType obr)
                                (maybeRef $ orFirstFrom obr)
                                (maybeRef $ orFirstTo obr)
                                (maybeRef (orFirstPropertyOffset obr,orFirstPropertyLength obr))

-- | A relation as represented in the relation file
data RelationRecord = RelationRecord
 { rrFrom          :: ObjectID  -- ^ origin object
 , rrFromType      :: ObjectTypeID -- ^ origin object type
 , rrTo            :: ObjectID -- ^ target object
 , rrToType        :: ObjectTypeID -- ^ target object type
 , rrType          :: RelationTypeID -- ^ type of the relation
 , rrFromNext      :: RelationID -- ^ next relation of origin object
 , rrToNext        :: RelationID -- ^ next relation of target object
 , rrFirstPropertyOffset :: PropertyValueOffset -- ^ first property id
 , rrFirstPropertyLength :: PropertyValueLength -- ^ first property id
 } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary RelationRecord

-- | simple default instance
instance Default RelationRecord where
 def  = RelationRecord 0 0 0 0 0 0 0 0 0

-- | Storable dictionary
storeRelation :: Store.Dictionary RelationRecord
storeRelation = Store.run $
 RelationRecord
    <$> Store.element rrFrom
    <*> Store.element rrFromType
    <*> Store.element rrTo
    <*> Store.element rrToType
    <*> Store.element rrType
    <*> Store.element rrFromNext
    <*> Store.element rrToNext
    <*> Store.element rrFirstPropertyOffset
    <*> Store.element rrFirstPropertyLength

-- | Storable instance
instance Storable RelationRecord where
   sizeOf = Store.sizeOf storeRelation
   alignment = Store.alignment storeRelation
   peek = Store.peek storeRelation
   poke = Store.poke storeRelation

-- | size of a relation record
relationSize :: Int64
relationSize =  binLength (def::RelationRecord)

instance HasID Relation RelationID where
  toNumID db = read $ drop 1 (keyObjDBRef db)
  toStringID i = "r" <> show i

data Relation = Relation
  { rID            :: RelationID
  , rFrom          :: DBRef Object  -- ^ origin object
  , rTo            :: DBRef Object -- ^ target object
  , rType          :: RelationTypeID -- ^ type of the relation
  , rFromNext      :: Maybe (DBRef Relation) -- ^ next relation of origin object
  , rToNext        :: Maybe (DBRef Relation) -- ^ next relation of target object
  , rFirstProperty :: Maybe (DBRef Property) -- ^ first property id
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance IResource Relation where
  keyResource = toStringID . rID
  writeResource Relation{..} = do
    hs <- readIORef handleRef
    let (fid,ftype) = toNumID rFrom
        (tid,ttype) = toNumID rTo
        (off,len) = maybeID rFirstProperty
        rr = RelationRecord fid ftype tid ttype rType (maybeID rFromNext) (maybeID rToNext) off len
        h = mhRelations hs
    pokeMM h rr (fromIntegral (rID - 1) * sizeOf rr)
  readResourceByKey k = do
    hs <- readIORef handleRef
    let rid = read $ drop 1 k
        h = mhRelations hs
    rr <- peekMM h (fromIntegral rid)
    if rrFrom rr == def
      then return Nothing
      else return $ Just $ Relation rid (getDBRef $ toStringID (rrFrom rr,rrFromType rr))
                                 (getDBRef $ toStringID (rrTo rr,rrToType rr))
                                 (rrType rr)
                                 (maybeRef $ rrFromNext rr)
                                 (maybeRef $ rrToNext rr)
                                 (maybeRef (rrFirstPropertyOffset rr,rrFirstPropertyLength rr))

-- | A property as represented in the property file
data Property = Property
  { pID     :: (PropertyValueOffset,PropertyValueLength)
  , pType   :: PropertyTypeID -- ^ type of the property
  , pNext   :: Maybe (DBRef Property) -- ^ next property
  , pValue  :: PropertyValue -- ^ property value
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance HasID Property (PropertyValueOffset,PropertyValueLength) where
  toNumID db = read $ map f $ drop 1 (keyObjDBRef db)
    where
      f 'a'=','
      f 'b'='('
      f 'c'=')'
      f a = a
  toStringID i = "p" <> map f (show i)
    where
      f ','='a'
      f '('='b'
      f ')'='c'
      f a = a

instance Indexable Property where
  key = toStringID . pID

-- instance Default PropertyRecord where
--   def = PropertyRecord def def def def def

-- instance IResource Property where
--   keyResource = toStringID . pID
--   writeResource Property{..} = do
--     hs <- readIORef handleRef
--     let pr = PropertyRecord pType (dataTypeID $ valueType pValue) (maybeID pNext) pOffset pLength
--         h = mhProperties hs
--         bs = toBin pValue
--     pokeMMBS (mhPropertyValues hs) bs (fromIntegral pOffset)
--     pokeMM h pr (fromIntegral (pID - 1) * sizeOf pr)
--   readResourceByKey k = do
--     hs <- readIORef handleRef
--     let pid = read $ drop 1 k
--         h = mhProperties hs
--     pr <- peekMM h ((fromIntegral pid - 1) * sizeOf (def::PropertyRecord))
--     if pr == def
--       then return Nothing
--       else do
--         let dt = dataTypeFromID $ prDataType pr
--         let r = maybeRef $ prNext pr
--         v <- toValue dt <$> peekMMBS (mhPropertyValues hs) (fromIntegral (prOffset pr)) (fromIntegral (prLength pr))
--         return $ Just $ Property pid (prType pr) r v (prOffset pr) (prLength pr)

-- instance IResource Property where
--   keyResource = toStringID . pID
--   writeResource Property{..} = do
--     hs <- readIORef handleRef
--     let b1 = runPut $ do
--                putInt16be pType
--                put (dataTypeID $ valueType pValue)
--                put (maybeID pNext)
--                put $ toBin pValue
--         h = mhProperties hs
--     print $ "writing property " ++ show pID
--     pokeMMBS h b1 (fromIntegral $ fst pID)
--     print $ "wrote property " ++ show pID
--     -- pokeMM h pr (fromIntegral (pID - 1) * sizeOf pr)
--   readResourceByKey k = do
--     hs <- readIORef handleRef
--     let pid = read $ drop 1 k
--         h = mhProperties hs
--     b <- peekMMBS h (fromIntegral $ fst pid) (fromIntegral $ snd pid)
--     return $ case runGetOrFail (decodeP pid) b of
--       Left _ -> Nothing
--       Right (_,_,m) -> m
--     where
--       decodeP pid = do
--         t <- get
--         if t == def
--           then return Nothing
--           else do
--             dt <- dataTypeFromID <$> get
--             next <- maybeRef <$> get
--             bs <- get
--             return $ Just $ Property pid t next (toValue dt bs)


    -- pr <- peekMM h ((fromIntegral pid - 1) * sizeOf (def::PropertyRecord))
    -- if pr == def
    --   then return Nothing
    --   else do
    --     let dt = dataTypeFromID $ prDataType pr
    --     let r = maybeRef $ prNext pr
    --     v <- toValue dt <$> peekMMBS (mhPropertyValues hs) (fromIntegral (prOffset pr)) (fromIntegral (prLength pr))
    --     return $ Just $ Property pid (prType pr) r v (prOffset pr) (prLength pr)

instance Serializable Property where
  serialize Property{..} = runPut $ do
                  putInt16be pType
                  put (dataTypeID $ valueType pValue)
                  put (maybeID pNext)
                  put $ toBin pValue
  deserialKey k = runGet $ decodeP (read $ map f $ drop 1 k)
    where
        decodeP pid = do
          t <- get
          dt <- dataTypeFromID <$> get
          next <- maybeRef <$> get
          bs <- get
          return $ Property pid t next (toValue dt bs)
        f 'a'=','
        f 'b'='('
        f 'c'=')'
        f a = a

-- | Handles to the various files
data Handles = MMHandles
  {
    mhObjects        :: MMapHandle ObjectRecord
  , hObjectFree      :: FreeList ObjectID
  , mhRelations      :: MMapHandle RelationRecord
  , hRelationFree    :: FreeList RelationID
  , mhProperties     :: MMapHandle Word8
  , hPropertyFree    :: FreeList PropertyID
  } -- ^ MMap Handles

handleRef :: IORef Handles
{-# NOINLINE handleRef #-}
handleRef = unsafePerformIO (newIORef (MMHandles undefined undefined undefined undefined undefined undefined))

setHandles :: Handles -> IO()
setHandles = writeIORef handleRef
