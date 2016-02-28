{-# LANGUAGE DeriveGeneric, FlexibleContexts, MultiParamTypeClasses, RecordWildCards, FunctionalDependencies, TypeSynonymInstances    #-}
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
type ObjectID       = Int32

-- | IDs for types of objects
type ObjectTypeID   = Int16

-- | IDs for relations
type RelationID     = Int32

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


class (Num typ,Eq typ,Show typ) => HasID dat typ | dat -> typ, typ -> dat where
  toNumID :: DBRef dat -> typ
  toStringID :: typ -> String

maybeID :: (HasID dat typ) => Maybe (DBRef dat) -> typ
maybeID Nothing = 0
maybeID (Just db)= toNumID db

maybeRef :: (HasID dat typ,IResource dat,Typeable dat) => typ -> Maybe (DBRef dat)
maybeRef nb
  | nb == 0 = Nothing
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
  , miProperty :: PropertyID
  , miValueOffset :: PropertyValueOffset
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Serializable MaxIDs where
   serialize= pack . show
   deserialize= read . unpack

instance Indexable MaxIDs where
   key = const maxName

instance Default MaxIDs where
  def = MaxIDs def def def def

data MetaData = MetaData
  { mdModel :: DBRef Model
  , mdMaxIDs :: DBRef MaxIDs
  }

metaData :: MetaData
metaData = MetaData (getDBRef modelName) (getDBRef maxName)

-- data APIModel = APIModel
--   { amModel :: DBRef Model
--   , amObjectTypes   :: Lookup ObjectTypeID T.Text
--   , amRelationTypes :: Lookup RelationTypeID T.Text
--   , amPropertyTypes :: Lookup PropertyTypeID (T.Text,DataType)
--   } deriving (Show,Read,Eq,Ord,Typeable)

-- | A property as represented in the property file
data Property = Property
  { pID     :: PropertyID
  , pType   :: PropertyTypeID -- ^ type of the property
  , pNext   :: Maybe (DBRef Property) -- ^ next property
  , pValue  :: PropertyValue -- ^ property value
  , pOffset :: PropertyValueOffset -- ^ offset of the value
  , pLength :: PropertyValueLength -- ^ length of the value
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- instance Indexable Property where
--  key p = toStringID (pID p)

data PropertyRecord = PropertyRecord
  { prType :: PropertyTypeID
  , prDataType :: DataTypeID
  , prNext :: PropertyID
  , prOffset :: PropertyValueOffset -- ^ offset of the value
  , prLength :: PropertyValueLength -- ^ length of the value
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Storable dictionary
storeProperty :: Store.Dictionary PropertyRecord
storeProperty = Store.run $
  PropertyRecord
     <$> Store.element prType
     <*> Store.element prDataType
     <*> Store.element prNext
     <*> Store.element prOffset
     <*> Store.element prLength

-- | Storable instance
instance Storable PropertyRecord where
    sizeOf = Store.sizeOf storeProperty
    alignment = Store.alignment storeProperty
    peek = Store.peek storeProperty
    poke = Store.poke storeProperty

instance HasID Property PropertyID where
  toNumID db = read $ drop 1 (keyObjDBRef db)
  toStringID i = "p" <> show i

instance Default PropertyRecord where
  def = PropertyRecord def def def def def

instance IResource Property where
  keyResource = toStringID . pID
  writeResource Property{..} = do
    hs <- readIORef handleRef
    let pr = PropertyRecord pType (dataTypeID $ valueType pValue) (maybeID pNext) pOffset pLength
        h = mhProperties hs
        bs = toBin pValue
    pokeMMBS (mhPropertyValues hs) bs (fromIntegral pOffset)
    pokeMM h pr (fromIntegral (pID - 1) * sizeOf pr)
  readResourceByKey k = do
    hs <- readIORef handleRef
    let pid = read $ drop 1 k
        h = mhProperties hs
    pr <- peekMM h ((fromIntegral pid - 1) * sizeOf (def::PropertyRecord))
    if pr == def
      then return Nothing
      else do
        let dt = dataTypeFromID $ prDataType pr
        let r = maybeRef $ prNext pr
        v <- toValue dt <$> peekMMBS (mhPropertyValues hs) (fromIntegral (prOffset pr)) (fromIntegral (prLength pr))
        return $ Just $ Property pid (prType pr) r v (prOffset pr) (prLength pr)

-- instance Serializable Property where
--   serialize Property{..} = runPut $ do
--     putInt32be pID
--     putInt16be pType
--     putInt32be (maybeID pNext)
--     putInt64be pOffset
--     putInt64be pLength
--   deserialize = runGet $ Property
--       <$> get
--       <*> get
--       <*> (maybeRef <$> get)
--       <*> get
--       <*> get

-- | Handles to the various files
data Handles = MMHandles
  {
  -- mhObjects        :: MMapHandle Object
  -- , hObjectFree      :: FreeList ObjectID
  --, mhRelations      :: MMapHandle Relation
  --, hRelationFree    :: FreeList RelationID
    mhProperties     :: MMapHandle PropertyRecord
  , hPropertyFree    :: FreeList PropertyID
  , mhPropertyValues :: MMapHandle Word8
  } -- ^ MMap Handles

handleRef :: IORef Handles
{-# NOINLINE handleRef #-}
handleRef = unsafePerformIO (newIORef (MMHandles undefined undefined undefined))

setHandles :: Handles -> IO()
setHandles = writeIORef handleRef
