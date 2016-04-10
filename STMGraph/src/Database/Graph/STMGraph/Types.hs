{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
-----------------------------------------------------------------------------
--
-- Module      :  Database.Graph.STMGraph.Types
-- Copyright   :
-- License     :  AllRightsReserved
--
-- Maintainer  :
-- Stability   :
-- Portability :
--
-- |
--
-----------------------------------------------------------------------------

module Database.Graph.STMGraph.Types where

import Database.Graph.STMGraph.LowLevel.MMapHandle

import Data.Int
import Foreign.Storable
import           Data.Default
import             Data.Binary
import Foreign.Storable.Record  as Store
import Data.Typeable
import GHC.Generics
import qualified Data.ByteString.Lazy as BS
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import qualified Data.Text as T
import           Data.Bits
import qualified Data.Map                               as DM
import qualified STMContainers.Map as SM
import qualified STMContainers.Set as SS
import Control.Concurrent
import Control.Concurrent.STM.TVar
import Control.Concurrent.MVar
import Control.Monad.STM
import Control.Concurrent.STM.TChan
import           System.IO
import qualified ListT
import Data.Hashable
import Focus

-- | IDs for objects
type ObjectID       = Int64

-- | IDs for types of objects
type ObjectTypeID   = Int16

-- | IDs for relations
type RelationID     = Int64

-- | IDs for types of relations
type RelationTypeID = Int16

-- | IDs for property values
type PropertyID     = Int64

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

data ObjectType = ObjectType
  { otID :: ObjectTypeID
  , otName :: T.Text
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)


-- | Type of a property as represented in the property type file
data PropertyType = PropertyType
  { ptID            :: PropertyTypeID -- ^ ID
  , ptDataType      :: DataType -- ^ Data type
  , ptName          :: T.Text -- ^ property name
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)


data RelationType = RelationType
  { rtID :: RelationTypeID
  , rtName :: T.Text
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)



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

lookupFromList :: (Ord a, Ord b) => [(a,b)] -> Lookup a b
lookupFromList  = foldr go def
    where
        -- ensure duplicates in list don't cause issues
        go (a,b) (Lookup fn tn) = let
            ma1 = DM.lookup b fn
            mb1 = DM.lookup a tn
            tn2 = rm ma1 tn
            fn2 = rm mb1 fn
            in Lookup (DM.insert b a fn2) (DM.insert a b tn2)
        rm Nothing m = m
        rm (Just a) m = DM.delete a m


data Model = Model
  { mObjectTypes   :: Lookup ObjectTypeID T.Text
  , mRelationTypes :: Lookup RelationTypeID T.Text
  , mPropertyTypes :: Lookup PropertyTypeID (T.Text,DataType)
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

modelToString :: Model -> String
modelToString Model{..} = show (DM.toAscList $ toName mObjectTypes,
    DM.toAscList $ toName mRelationTypes,
    DM.toAscList $ toName mPropertyTypes)

stringToModel :: String -> Model
stringToModel s
    | null s = def
    | [((lo,lr,lp),"")] <- reads s= Model (lookupFromList lo) (lookupFromList lr) (lookupFromList lp)
    | otherwise = error "fail to read Model from string"

instance Default Model where
  def = Model def def def

--data IDGen = IDGen
--    { maxID :: TVar Int64
--    ,  freeIDs :: SM.Map Int64 Int64
--    }
--
--nextID ::  IDGen -> Int64 -> STM Int64
--nextID IDGen{..} l = do
--    go $ SM.stream freeIDs
--    where
--        go ls = do
--            ml <- ListT.uncons ls
--            case ml of
--                Nothing -> do
--                    mi<-readTVar maxID
--                    let ni=mi+l
--                    writeTVar maxID ni
--                    return mi
--                Just ((i,il),_) | l == il -> do
--                    SM.delete i freeIDs
--                    return i
--                Just ((i,il),rs) | l > il -> go rs
--                Just ((i,il),_) | l < il -> do
--                    SM.delete i freeIDs
--                    SM.insert (il-l) (i+l) freeIDs
--                    return i
--
--freeID :: Int64 -> Int64 -> IDGen -> STM ()
--freeID i l IDGen{..} = do
--    mi<-readTVar maxID
--    if i==mi
--        then do
--            li<-removeLast (mi-l)
--            writeTVar maxID li
--        else do
--            SM.insert l i freeIDs
--    where
--        removeLast i = do
--            mf <- SM.focus (\m->return (fmap (\l-> i-l) m,Remove)) i freeIDs
--            case mf of
--                Just a -> removeLast a
--                Nothing -> return i
--
-- newIDGen :: Int64 -> STM IDGen
-- newIDGen st= IDGen <$> newTVar st <*> SM.new


data IDGen = IDGen
    { maxID :: TVar Int64
    ,  freeIDs :: TVar (DM.Map Int64 Int64)
    }

nextID ::  IDGen -> Int64 -> STM Int64
nextID IDGen{..} l = do
    m <- readTVar freeIDs
    (m2,i) <- go m $ DM.toAscList m
    writeTVar freeIDs m2
    return i
    where
        go m [] = do
                        mi<-readTVar maxID
                        let ni=mi+l
                        writeTVar maxID ni
                        return (m,mi)
        go m ((i,il):_) | l == il = do
                    let m2=DM.delete i m
                    return (m2,i)
        go m ((i,il):rs) | l > il = go m rs
        go m ((i,il):_) | l < il = do
                    let m2=DM.delete i m
                    let m3=DM.insert (il-l) (i+l) m2
                    return (m3,i)

freeID :: Int64 -> Int64 -> IDGen -> STM ()
freeID i l IDGen{..} = do
    mi <- readTVar maxID
    m <- readTVar freeIDs
    m2<-if i+l==mi
                    then do
                        let (m2,li)=removeLast m i
                        writeTVar maxID li
                        return m2
                    else do
                        let
                          mBef = DM.lookupLT i m
                          mAft = DM.lookupGT i m
                        return $ clean mi $ defrag mBef mAft i l m
    writeTVar freeIDs m2
    where
        removeLast m i =
           case DM.lookupLE i m of
                Just (ix,lx) | (ix+lx)==i -> (DM.delete ix m,ix)
                _ -> (m,i)
        defrag (Just (bx,bv)) (Just (ax,av)) i l m | (bx+bv==i) && (i+l==ax) = DM.insert bx (bv+l+av) $ DM.delete ax m
        defrag (Just (bx,bv)) _ i l m | bx+bv==i  = DM.insert bx (bv+l) m
        defrag _ (Just (ax,av)) i l m | i+l==ax = DM.insert i (l+av) $ DM.delete ax m
        defrag _ _ i l m = DM.insert i l m
        clean i m
            | DM.null m = m
            | otherwise =
                let (k,v) = DM.findMax m
                in if k>=i
                    then DM.delete k m
                    else m


newIDGen :: Int64 -> STM IDGen
newIDGen st= IDGen <$> newTVar st <*> newTVar DM.empty

data MetaData = MetaData
  { mdModel :: TVar Model
  ,  mdGenObjectID :: IDGen
  ,  mdGenRelationID :: IDGen
  ,  mdGenPropertyID :: IDGen
  ,  mdGenPropertyOffset :: IDGen
  }

newMetaData :: STM MetaData
newMetaData = MetaData
    <$> newTVar def
    <*> newIDGen 1
    <*> newIDGen 1
    <*> newIDGen 1
    <*> newIDGen 0


data GraphData = GraphData
  { gdObjects :: SM.Map ObjectID Object
  , gdRelations :: SM.Map RelationID Relation
  , gdProperties :: SM.Map PropertyID (Property,PropertyValue)
 }

newGraphData :: STM GraphData
newGraphData = GraphData
    <$> SM.new
    <*> SM.new
    <*> SM.new


data WriteEvent =
    WrittenModel Model
  | WrittenObject ObjectID Object
  | WrittenRelation RelationID Relation
  | WrittenProperty PropertyID (Property,BS.ByteString)
  | DeletedObject ObjectID
  | DeletedRelation RelationID
  | DeletedProperty PropertyID
  | ClosedDatabase
  | Checkpoint (MVar ())
  deriving (Eq,Typeable,Generic)

data Database = Database
  { dMetadata :: MetaData
  ,  dData :: GraphData
  ,  dWrites :: TChan WriteEvent
  ,  dWriterThread :: MVar ()
  }

newDatabase :: MVar() -> STM Database
newDatabase mv = Database
    <$> newMetaData
    <*> newGraphData
    <*> newTChan
    <*> pure mv

-- | Handles to the various files
data Handles =  Handles
  { hObjects        :: Handle
  , hRelations      :: Handle
  , hProperties     :: Handle
  , hPropertyValues :: Handle
  , hModel :: FilePath
  } -- ^ Direct Handles
--  |  MMHandles
--  { mhObjects        :: MMapHandle Object
--  , mhRelations      :: MMapHandle Relation
--  , mhProperties     :: MMapHandle Property
--  , mhPropertyValues :: MMapHandle Word8
--  , hModel :: FilePath
--  } -- ^ MMap Handles

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
