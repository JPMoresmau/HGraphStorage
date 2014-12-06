{-# LANGUAGE DeriveDataTypeable, DeriveGeneric, OverloadedStrings, ConstraintKinds #-}
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

-- | put our constraints in one synonym
type GraphUsableMonad m=(MonadBaseControl IO m, R.MonadResource m, MonadLogger m)


type ObjectID       = Int32

type ObjectTypeID   = Int16

type RelationID     = Int32

type RelationTypeID = Int16

type PropertyID     = Int32

type PropertyTypeID = Int16

type DataTypeID     = Int8

type PropertyValueOffset = Int32

type PropertyValueLength = Int64

data Object = Object
  {
    oType      :: ObjectTypeID
  , oFirstFrom :: RelationID
  , oFirstTo   :: RelationID
  , oFirstProperty :: PropertyID
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Binary Object

instance Default Object where
  def = Object 0 0 0 0

objectSize :: Int64
objectSize = binLength (def::Object)


binLength :: (Binary b) => b -> Int64 
binLength = BS.length . encode 


data Relation = Relation 
  { rFrom      :: ObjectID
  , rFromType  :: ObjectTypeID
  , rTo        :: ObjectID
  , rToType    :: ObjectTypeID
  , rType      :: RelationTypeID
  , rFromFrom  :: RelationID
  , rFromTo    :: RelationID
  , rToFrom    :: RelationID
  , rToTo      :: RelationID
  , rFirstProperty :: PropertyID
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Binary Relation
  
instance Default Relation where
  def  = Relation 0 0 0 0 0 0 0 0 0 0
  
relationSize :: Int64
relationSize =  binLength (def::Relation)

data Property = Property
  { pType      :: PropertyTypeID
  , pNext      :: PropertyID
  , pOffset    :: PropertyValueOffset
  , pLength    :: PropertyValueLength
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Binary Property

instance Default Property where
  def = Property 0 0 0 0

propertySize :: Int64
propertySize = binLength (def::Property)

data PropertyType = PropertyType
  { ptDataType :: DataTypeID
  , ptFirstProperty :: PropertyID
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Binary PropertyType

instance Default PropertyType where
  def = PropertyType 0 0

propertyTypeSize :: Int64
propertyTypeSize = binLength (def::PropertyType)

data ObjectType = ObjectType
  { otFirstProperty :: PropertyID
  }deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Binary ObjectType
  
instance Default ObjectType where
  def = ObjectType 0
  
objectTypeSize :: Int64
objectTypeSize = binLength (def::ObjectType)

data RelationType = RelationType
  { rtFirstProperty :: PropertyID
  }deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Binary RelationType
  
instance Default RelationType where
  def = RelationType 0
  
relationTypeSize :: Int64
relationTypeSize = binLength (def::RelationType)

data Handles = Handles 
  { hObjects        :: Handle
  , hObjectTypes    :: Handle
  , hRelations      :: Handle
  , hRelationTypes  :: Handle
  , hProperties     :: Handle
  , hPropertyTypes  :: Handle
  , hPropertyValues :: Handle
  }

data Model = Model
  { mObjectTypes   :: Lookup ObjectTypeID Text
  , mRelationTypes :: Lookup RelationTypeID Text
  , mPropertyTypes :: Lookup PropertyTypeID (Text,DataType)
  } deriving (Show,Read,Eq,Ord,Typeable)

instance Default Model where
  def = Model def def (Lookup (DM.singleton ("name",DTText) namePropertyID) (DM.singleton namePropertyID ("name",DTText)))

namePropertyID :: PropertyTypeID
namePropertyID = 1

data Lookup a b = Lookup 
  { fromName :: DM.Map b a
  , toName   :: DM.Map a b
  } deriving (Show,Read,Eq,Ord,Typeable)

instance Default (Lookup a b) where
  def = Lookup DM.empty DM.empty

addToLookup :: (Ord a, Ord b) => a -> b -> Lookup a b -> Lookup a b
addToLookup a b (Lookup fn tn) = Lookup (DM.insert b a fn) (DM.insert a b tn)


data DataType = DTText | DTInteger | DTBinary
  deriving (Show,Read,Eq,Ord,Bounded,Enum,Typeable)

dataTypeID :: DataType -> DataTypeID
dataTypeID = fromIntegral . fromEnum

dataType :: DataTypeID -> DataType
dataType = toEnum . fromIntegral

data PropertyValue = 
    PVText    Text
  | PVInteger Integer
  | PVBinary  BS.ByteString
  deriving (Show,Read,Eq,Ord,Typeable)
  
valueType :: PropertyValue -> DataType
valueType (PVText _) = DTText
valueType (PVInteger _) = DTInteger
valueType (PVBinary _) = DTBinary
 
data GraphStorageException = 
    IncoherentNamePropertyTypeID PropertyTypeID PropertyTypeID
  | UnknownPropertyType PropertyTypeID
  | NoNameProperty PropertyTypeID
  | MultipleNameProperty PropertyTypeID
  | UnknownObjectType ObjectTypeID
  | UnknownRelationType RelationTypeID
  deriving (Show,Read,Eq,Ord,Typeable)
  
instance Exception GraphStorageException
  