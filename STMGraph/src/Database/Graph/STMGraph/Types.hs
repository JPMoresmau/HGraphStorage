{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
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
import qualified Data.Aeson as A
import qualified Data.Aeson.Parser as A
import qualified Data.Attoparsec.ByteString as A

-- | IDs for Nodes
type NodeID       = Int64

-- | IDs for types of Nodes
type NodeTypeID   = Int16

-- | IDs for Edges
type EdgeID     = Int64

-- | IDs for types of Edges
type EdgeTypeID = Int16

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

-- | An Node as represented in the Node file
data Node = Node
  {
    nType          :: {-# UNPACK #-} !NodeTypeID -- ^ type of Node
  , nFirstFrom     :: {-# UNPACK #-} !EdgeID   -- ^ first Edge starting from the Node
  , nFirstTo       :: {-# UNPACK #-} !EdgeID   -- ^ first Edge arriving at the Node
  , nFirstProperty :: {-# UNPACK #-} !PropertyID -- ^ first property
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | Simple binary instance
instance Binary Node

-- | Simple default instance
instance Default Node where
  def = Node 0 0 0 0

-- | Storable dictionary
storeNode :: Store.Dictionary Node
storeNode = Store.run $
  Node
     <$> Store.element nType
     <*> Store.element nFirstFrom
     <*> Store.element nFirstTo
     <*> Store.element nFirstProperty

-- | Storable instance
instance Storable Node where
    sizeOf = Store.sizeOf storeNode
    alignment = Store.alignment storeNode
    peek = Store.peek storeNode
    poke = Store.poke storeNode

-- | Size of an Node record
nodeSize :: Int64
nodeSize = binLength (def::Node)

-- | Calculates the length of the binary serialization of the given Node
binLength :: (Binary b) => b -> Int64
binLength = BS.length . encode

-- | A Edge as represented in the Edge file
data Edge = Edge
  { eFrom          :: {-# UNPACK #-} !NodeID  -- ^ origin Node
  , eFromType      :: {-# UNPACK #-} !NodeTypeID -- ^ origin Node type
  , eTo            :: {-# UNPACK #-} !NodeID -- ^ target Node
  , eToType        :: {-# UNPACK #-} !NodeTypeID -- ^ target Node type
  , eType          :: {-# UNPACK #-} !EdgeTypeID -- ^ type of the Edge
  , eFromNext      :: {-# UNPACK #-} !EdgeID -- ^ next Edge of origin Node
  , eToNext        :: {-# UNPACK #-} !EdgeID -- ^ next Edge of target Node
  , eFirstProperty :: {-# UNPACK #-} !PropertyID -- ^ first property id
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

-- | simple binary instance
instance Binary Edge

-- | simple default instance
instance Default Edge where
  def  = Edge 0 0 0 0 0 0 0 0

-- | Storable dictionary
storeEdge :: Store.Dictionary Edge
storeEdge = Store.run $
  Edge
     <$> Store.element eFrom
     <*> Store.element eFromType
     <*> Store.element eTo
     <*> Store.element eToType
     <*> Store.element eType
     <*> Store.element eFromNext
     <*> Store.element eToNext
     <*> Store.element eFirstProperty

-- | Storable instance
instance Storable Edge where
    sizeOf = Store.sizeOf storeEdge
    alignment = Store.alignment storeEdge
    peek = Store.peek storeEdge
    poke = Store.poke storeEdge

-- | size of a Edge record
edgeSize :: Int64
edgeSize =  binLength (def::Edge)

-- | A property as represented in the property file
data Property = Property
  { pType   :: {-# UNPACK #-} !PropertyTypeID -- ^ type of the property
  , pNext   :: {-# UNPACK #-} !PropertyID -- ^ next property id
  , pOffset :: {-# UNPACK #-} !PropertyValueOffset -- ^ offset of the value
  , pLength :: {-# UNPACK #-} !PropertyValueLength -- ^ length of the value
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
data DataType = DTText | DTInteger | DTBinary | DTJSON
  deriving (Show,Read,Eq,Ord,Bounded,Enum,Typeable)

-- | Convert a DataType Node to its ID
dataTypeID :: DataType -> DataTypeID
dataTypeID = fromIntegral . fromEnum

-- | Convert a DataType ID to the Haskell Node
dataType :: DataTypeID -> DataType
dataType = toEnum . fromIntegral

-- | A typed property value
data PropertyValue =
    PVText    T.Text
  | PVInteger Integer
  | PVBinary  BS.ByteString
  | PVJSON    A.Value
  deriving (Show,Read,Eq,Typeable)

instance Default PropertyValue where
    def = PVBinary BS.empty

-- | Get the data type for a given value
valueType :: PropertyValue -> DataType
valueType (PVText _) = DTText
valueType (PVInteger _) = DTInteger
valueType (PVBinary _) = DTBinary
valueType (PVJSON _) = DTJSON

-- | Convert a property value to a bytestring
toBin :: PropertyValue -> BS.ByteString
toBin (PVBinary bs) = bs
toBin (PVText t) = BS.fromStrict $ encodeUtf8 t
toBin (PVInteger i) = encode i
toBin (PVJSON j) = A.encode j

-- | Convert a bytestring to a propertyvalue
toValue :: DataType -> BS.ByteString -> PropertyValue
toValue DTBinary  = PVBinary
toValue DTText    = PVText . decodeUtf8 . BS.toStrict
toValue DTInteger = PVInteger . decode
toValue DTJSON    = PVJSON . fm . A.decode
  where
      fm (Just r)= r
      fm _ = error "Typed.toValue: cannot parse JSON value"

data NodeType = NodeType
  { otID :: NodeTypeID
  , otName :: T.Text
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)


-- | Type of a property as represented in the property type file
data PropertyType = PropertyType
  { ptID            :: PropertyTypeID -- ^ ID
  , ptDataType      :: DataType -- ^ Data type
  , ptName          :: T.Text -- ^ property name
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)


data EdgeType = EdgeType
  { rtID :: EdgeTypeID
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
  { mNodeTypes   :: Lookup NodeTypeID T.Text
  , mEdgeTypes :: Lookup EdgeTypeID T.Text
  , mPropertyTypes :: Lookup PropertyTypeID (T.Text,DataType)
  } deriving (Show,Read,Eq,Ord,Typeable,Generic)

modelToString :: Model -> String
modelToString Model{..} = show (DM.toAscList $ toName mNodeTypes,
    DM.toAscList $ toName mEdgeTypes,
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
                    let m3=DM.insert (i+l) (il-l) m2
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
  ,  mdGenNodeID :: IDGen
  ,  mdGenEdgeID :: IDGen
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
  { gdNodes :: SM.Map NodeID Node
  , gdEdges :: SM.Map EdgeID Edge
  , gdProperties :: SM.Map PropertyID (Property,PropertyValue)
 }

newGraphData :: STM GraphData
newGraphData = GraphData
    <$> SM.new
    <*> SM.new
    <*> SM.new


data WriteEvent =
    WrittenModel Model
  | WrittenNode NodeID Node
  | WrittenEdge EdgeID Edge
  | WrittenProperty PropertyID (Property,BS.ByteString)
  | DeletedNode NodeID
  | DeletedEdge EdgeID
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
  { hNodes        :: Handle
  , hEdges      :: Handle
  , hProperties     :: Handle
  , hPropertyValues :: Handle
  , hModel :: FilePath
  } -- ^ Direct Handles
--  |  MMHandles
--  { mhNodes        :: MMapHandle Node
--  , mhEdges      :: MMapHandle Edge
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
