{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverloadedStrings, ConstraintKinds #-}
-- | Base types and simple functions on them
module Database.Graph.HGraphStorage.Types where

import Control.Exception.Base
import Data.Binary
import qualified Data.ByteString.Lazy as BS
import Data.Int
import Data.Typeable
import Data.Default
import qualified Data.Map as DM
import Data.Text

import GHC.Generics (Generic)
import System.IO
import Control.Monad.Logger (MonadLogger)
import Control.Monad.Trans.Control ( MonadBaseControl )
import qualified Control.Monad.Trans.Resource as R

import Database.Graph.HGraphStorage.Constants

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
    oType      :: ObjectTypeID -- ^ type of object
  , oFirstFrom :: RelationID   -- ^ first relation starting from the object
  , oFirstTo   :: RelationID   -- ^ first relation arriving at the object
  , oFirstProperty :: PropertyID -- ^ first property
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Simple binary instance
instance Binary Object

-- | Simple default instance
instance Default Object where
  def = Object 0 0 0 0

-- | Size of an object record
objectSize :: Int64
objectSize = binLength (def::Object)

-- | Calculates the length of the binary serialization of the given object
binLength :: (Binary b) => b -> Int64 
binLength = BS.length . encode 

-- | A relation as represented in the relation file
data Relation = Relation 
  { rFrom      :: ObjectID  -- ^ origin object
  , rFromType  :: ObjectTypeID -- ^ origin object type
  , rTo        :: ObjectID -- ^ target object
  , rToType    :: ObjectTypeID -- ^ target object type
  , rType      :: RelationTypeID -- ^ type of the relation
  , rFromNext  :: RelationID -- ^ next relation of origin object
  , rToNext    :: RelationID -- ^ next relation of target object
  , rFirstProperty :: PropertyID -- ^ first property id
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary Relation
  
-- | simple default instance
instance Default Relation where
  def  = Relation 0 0 0 0 0 0 0 0
  
-- | size of a relation record
relationSize :: Int64
relationSize =  binLength (def::Relation)

-- | A property as represented in the property file
data Property = Property
  { pType      :: PropertyTypeID -- ^ type of the property
  , pNext      :: PropertyID -- ^ next property id
  , pOffset    :: PropertyValueOffset -- ^ offset of the value
  , pLength    :: PropertyValueLength -- ^ length of the value
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary Property

-- | simple default instance
instance Default Property where
  def = Property 0 0 0 0

-- | size of a property record
propertySize :: Int64
propertySize = binLength (def::Property)

-- | Type of a property as represented in the property type file
data PropertyType = PropertyType
  { ptDataType :: DataTypeID -- ^ Data type ID
  , ptFirstProperty :: PropertyID -- ^ first property of the type itself
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary PropertyType

-- | simple default instance
instance Default PropertyType where
  def = PropertyType 0 0

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
  
-- | Size of an object type record
objectTypeSize :: Int64
objectTypeSize = binLength (def::ObjectType)

-- | Type of a relation as represented in the relation type file
data RelationType = RelationType
  { rtFirstProperty :: PropertyID -- ^ First property of the type itself
  }deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary RelationType
  
-- | simple default instance
instance Default RelationType where
  def = RelationType 0
  
-- | Size of a relation type record
relationTypeSize :: Int64
relationTypeSize = binLength (def::RelationType)

-- | Handles to the various files
data Handles = Handles 
  { hObjects        :: Handle
  , hObjectTypes    :: Handle
  , hRelations      :: Handle
  , hRelationTypes  :: Handle
  , hProperties     :: Handle
  , hPropertyTypes  :: Handle
  , hPropertyValues :: Handle
  }

-- | The current model: lookup tables between names and ids types of artifacts
data Model = Model
  { mObjectTypes   :: Lookup ObjectTypeID Text
  , mRelationTypes :: Lookup RelationTypeID Text
  , mPropertyTypes :: Lookup PropertyTypeID (Text,DataType)
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
    PVText    Text
  | PVInteger Integer
  | PVBinary  BS.ByteString
  deriving (Show,Read,Eq,Ord,Typeable)
  
-- | Get the data type for a given value
valueType :: PropertyValue -> DataType
valueType (PVText _) = DTText
valueType (PVInteger _) = DTInteger
valueType (PVBinary _) = DTBinary
 
-- | The exceptions we may throw
data GraphStorageException = 
    IncoherentNamePropertyTypeID PropertyTypeID PropertyTypeID -- ^ Something is not right with the name property
  | UnknownPropertyType PropertyTypeID
  | NoNameProperty PropertyTypeID
  | MultipleNameProperty PropertyTypeID
  | UnknownObjectType ObjectTypeID
  | UnknownRelationType RelationTypeID
  deriving (Show,Read,Eq,Ord,Typeable)
  
-- | Make our exception a standard exception
instance Exception GraphStorageException

  