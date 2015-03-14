{-# LANGUAGE ConstraintKinds    #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
-- | Base types and simple functions on them
module Database.Graph.HGraphStorage.Types where

import           Control.Applicative
import           Control.Exception.Base
import           Data.Binary
import           Data.Bits
import qualified Data.ByteString.Lazy                   as BS
import           Data.Default
import           Data.Int
import qualified Data.Map                               as DM
import qualified Data.Text                              as T
import           Data.Typeable

import           Control.Monad.Logger                   (MonadLogger)
import           Control.Monad.Trans.Control            (MonadBaseControl)
import qualified Control.Monad.Trans.Resource           as R
import           GHC.Generics                           (Generic)
import           System.IO
import Foreign.Storable.Record  as Store

import           Database.Graph.HGraphStorage.Constants
import           Database.Graph.HGraphStorage.FreeList
import           Database.Graph.HGraphStorage.LowLevel.MMapHandle
import Foreign.Storable

-- | put our constraints in one synonym
type GraphUsableMonad m=(MonadBaseControl IO m, R.MonadResource m, MonadLogger m)

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

-- | An object as represented in the object file
data Object = Object
  {
    oType          :: ObjectTypeID -- ^ type of object
  , oFirstFrom     :: RelationID   -- ^ first relation starting from the object
  , oFirstTo       :: RelationID   -- ^ first relation arriving at the object
  , oFirstProperty :: PropertyID -- ^ first property
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Simple binary instance
instance Binary Object

-- | Simple default instance
instance Default Object where
  def = Object 0 0 0 0

-- | Storable dictionary
storeObject :: Store.Dictionary Object
storeObject = Store.run $
  Object 
     <$> Store.element oType
     <*> Store.element oFirstFrom
     <*> Store.element oFirstTo
     <*> Store.element oFirstProperty

-- | Storable instance
instance Storable Object where
    sizeOf = Store.sizeOf storeObject
    alignment = Store.alignment storeObject
    peek = Store.peek storeObject
    poke = Store.poke storeObject

-- | Size of an object record
objectSize :: Int64
objectSize = binLength (def::Object)

-- | Calculates the length of the binary serialization of the given object
binLength :: (Binary b) => b -> Int64
binLength = BS.length . encode

-- | A relation as represented in the relation file
data Relation = Relation
  { rFrom          :: ObjectID  -- ^ origin object
  , rFromType      :: ObjectTypeID -- ^ origin object type
  , rTo            :: ObjectID -- ^ target object
  , rToType        :: ObjectTypeID -- ^ target object type
  , rType          :: RelationTypeID -- ^ type of the relation
  , rFromNext      :: RelationID -- ^ next relation of origin object
  , rToNext        :: RelationID -- ^ next relation of target object
  , rFirstProperty :: PropertyID -- ^ first property id
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary Relation

-- | simple default instance
instance Default Relation where
  def  = Relation 0 0 0 0 0 0 0 0

-- | Storable dictionary
storeRelation :: Store.Dictionary Relation
storeRelation = Store.run $
  Relation 
     <$> Store.element rFrom
     <*> Store.element rFromType
     <*> Store.element rTo
     <*> Store.element rToType
     <*> Store.element rType
     <*> Store.element rFromNext
     <*> Store.element rToNext
     <*> Store.element rFirstProperty

-- | Storable instance
instance Storable Relation where
    sizeOf = Store.sizeOf storeRelation
    alignment = Store.alignment storeRelation
    peek = Store.peek storeRelation
    poke = Store.poke storeRelation

-- | size of a relation record
relationSize :: Int64
relationSize =  binLength (def::Relation)

-- | A property as represented in the property file
data Property = Property
  { pType   :: PropertyTypeID -- ^ type of the property
  , pNext   :: PropertyID -- ^ next property id
  , pOffset :: PropertyValueOffset -- ^ offset of the value
  , pLength :: PropertyValueLength -- ^ length of the value
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary Property

-- | simple default instance
instance Default Property where
  def = Property 0 0 0 0

-- | Storable dictionary
storeProperty :: Store.Dictionary Property
storeProperty = Store.run $
  Property 
     <$> Store.element pType
     <*> Store.element pNext
     <*> Store.element pOffset
     <*> Store.element pLength

-- | Storable instance
instance Storable Property where
    sizeOf = Store.sizeOf storeProperty
    alignment = Store.alignment storeProperty
    peek = Store.peek storeProperty
    poke = Store.poke storeProperty

-- | size of a property record
propertySize :: Int64
propertySize = binLength (def::Property)

-- | Type of a property as represented in the property type file
data PropertyType = PropertyType
  { ptDataType      :: DataTypeID -- ^ Data type ID
  , ptFirstProperty :: PropertyID -- ^ first property of the type itself
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary PropertyType

-- | simple default instance
instance Default PropertyType where
  def = PropertyType 0 0

-- | Storable dictionary
storePropertyType :: Store.Dictionary PropertyType
storePropertyType = Store.run $
  PropertyType 
     <$> Store.element ptDataType
     <*> Store.element ptFirstProperty

-- | Storable instance
instance Storable PropertyType where
    sizeOf = Store.sizeOf storePropertyType
    alignment = Store.alignment storePropertyType
    peek = Store.peek storePropertyType
    poke = Store.poke storePropertyType

-- | size of a property type record
propertyTypeSize :: Int64
propertyTypeSize = binLength (def::PropertyType)

-- | Type of an object as represented in the object type file
data ObjectType = ObjectType
  { otFirstProperty :: PropertyID -- ^ First property of the type itself
  }deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary ObjectType

-- | simple default instance
instance Default ObjectType where
  def = ObjectType 0

-- | Storable dictionary
storeObjectType :: Store.Dictionary ObjectType
storeObjectType = Store.run $
  ObjectType 
     <$> Store.element otFirstProperty

-- | Storable instance
instance Storable ObjectType where
    sizeOf = Store.sizeOf storeObjectType
    alignment = Store.alignment storeObjectType
    peek = Store.peek storeObjectType
    poke = Store.poke storeObjectType

-- | Size of an object type record
objectTypeSize :: Int64
objectTypeSize = binLength (def::ObjectType)

-- | Type of a relation as represented in the relation type file
data RelationType = RelationType
  { rtFirstProperty :: PropertyID -- ^ First property of the type itself
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary RelationType

-- | simple default instance
instance Default RelationType where
  def = RelationType 0

-- | Storable dictionary
storeRelationType :: Store.Dictionary RelationType
storeRelationType = Store.run $
  RelationType 
     <$> Store.element rtFirstProperty

-- | Storable instance
instance Storable RelationType where
    sizeOf = Store.sizeOf storeRelationType
    alignment = Store.alignment storeRelationType
    peek = Store.peek storeRelationType
    poke = Store.poke storeRelationType

-- | Size of a relation type record
relationTypeSize :: Int64
relationTypeSize = binLength (def::RelationType)

-- | Handles to the various files
data Handles = Handles
  { hObjects        :: Handle
  , hObjectFree     :: FreeList ObjectID
  , hObjectTypes    :: Handle
  , hRelations      :: Handle
  , hRelationFree   :: FreeList RelationID
  , hRelationTypes  :: Handle
  , hProperties     :: Handle
  , hPropertyFree   :: FreeList PropertyID
  , hPropertyTypes  :: Handle
  , hPropertyValues :: Handle
  } -- ^ Direct Handles
  | MMHandles
  { mhObjects        :: MMapHandle Object
  , hObjectFree      :: FreeList ObjectID
  , mhObjectTypes    :: MMapHandle ObjectType
  , mhRelations      :: MMapHandle Relation
  , hRelationFree    :: FreeList RelationID
  , mhRelationTypes  :: MMapHandle RelationType
  , mhProperties     :: MMapHandle Property
  , hPropertyFree    :: FreeList PropertyID
  , mhPropertyTypes  :: MMapHandle PropertyType
  , mhPropertyValues :: MMapHandle Word8
  , mhMaxIDs         :: MMapHandle MaxIDs
  } -- ^ MMap Handles

-- | Maximum ids used for objects
data MaxIDs = MaxIDs 
  { miObject :: ObjectID
  , miObjectType :: ObjectTypeID
  , miRelation :: RelationID
  , miRelationType :: RelationTypeID
  , miProperty :: PropertyID
  , miPropertyType :: PropertyTypeID
  , miPropertyOffset :: PropertyValueOffset
  }  deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary MaxIDs


-- | simple default instance
instance Default MaxIDs where
  def = MaxIDs def def def def def def def

-- | Storable dictionary
storeMaxIDs :: Store.Dictionary MaxIDs
storeMaxIDs = Store.run $
  MaxIDs 
     <$> Store.element miObject
     <*> Store.element miObjectType
     <*> Store.element miRelation
     <*> Store.element miRelationType
     <*> Store.element miProperty
     <*> Store.element miPropertyType
     <*> Store.element miPropertyOffset

-- | Storable instance
instance Storable MaxIDs where
    sizeOf = Store.sizeOf storeMaxIDs
    alignment = Store.alignment storeMaxIDs
    peek = Store.peek storeMaxIDs
    poke = Store.poke storeMaxIDs
 
  
-- | The current model: lookup tables between names and ids types of artifacts
data Model = Model
  { mObjectTypes   :: Lookup ObjectTypeID T.Text
  , mRelationTypes :: Lookup RelationTypeID T.Text
  , mPropertyTypes :: Lookup PropertyTypeID (T.Text,DataType)
  } deriving (Show,Read,Eq,Ord,Typeable)

-- | Default model: a "name" property property type with a name property
instance Default Model where
  def = Model def def (Lookup (DM.singleton (namePropertyName,DTText) namePropertyID) (DM.singleton namePropertyID (namePropertyName,DTText)))

-- | the ID of the "name" property
namePropertyID :: PropertyTypeID
namePropertyID = 1



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

-- | Convert a DataType object to its ID
dataTypeID :: DataType -> DataTypeID
dataTypeID = fromIntegral . fromEnum

-- | Convert a DataType ID to the Haskell object
dataType :: DataTypeID -> DataType
dataType = toEnum . fromIntegral

-- | A typed property value
data PropertyValue =
    PVText    T.Text
  | PVInteger Integer
  | PVBinary  BS.ByteString
  deriving (Show,Read,Eq,Ord,Typeable)

-- | Get the data type for a given value
valueType :: PropertyValue -> DataType
valueType (PVText _) = DTText
valueType (PVInteger _) = DTInteger
valueType (PVBinary _) = DTBinary

-- | Get the value in a format ready to index
valueToIndex :: PropertyValue -> [Int16]
valueToIndex (PVText t) = textToKey t
valueToIndex (PVInteger i) = integerToKey i
valueToIndex (PVBinary b) = bytestringToKey b


-- | Transform a text in a index key
textToKey :: T.Text  -> [Int16]
textToKey = map (fromIntegral . fromEnum) . T.unpack

-- | Transform an integer in a index key
integerToKey :: Integer -> [Int16]
integerToKey i = if i < maxI && i > minI
  then [fromIntegral i]
  else integerToKey (i `shift` (-16)) ++ [fromIntegral $ i .&. 0xFFFF]
  where
    maxI = fromIntegral (maxBound :: Int16)
    minI = fromIntegral (minBound :: Int16)

-- | Transform a bytestring in a index key
bytestringToKey :: BS.ByteString -> [Int16]
bytestringToKey = map fromIntegral . BS.unpack


-- | The exceptions we may throw
data GraphStorageException =
    IncoherentNamePropertyTypeID PropertyTypeID PropertyTypeID -- ^ Something is not right with the name property
  | UnknownPropertyType PropertyTypeID
  | NoNameProperty PropertyTypeID
  | MultipleNameProperty PropertyTypeID
  | UnknownObjectType ObjectTypeID
  | UnknownRelationType RelationTypeID
  | DuplicateIndexKey [T.Text]
  deriving (Show,Read,Eq,Ord,Typeable)

-- | Make our exception a standard exception
instance Exception GraphStorageException

-- | Settings for the Graph DB
data GraphSettings = GraphSettings
  { gsMainBuffering  :: Maybe BufferMode
  , gsFreeBuffering  :: Maybe BufferMode
  , gsIndexBuffering :: Maybe BufferMode
  , gsUseMMap        :: Bool -- ^ Use MMap or not?
  } deriving (Show,Read,Eq,Ord,Typeable)

-- | Default instance for settings
instance Default GraphSettings where
  def = GraphSettings Nothing Nothing Nothing True

