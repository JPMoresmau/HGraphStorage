{-# LANGUAGE DeriveDataTypeable #-}
module Database.Graph.HGraphStorage.Graph where

import qualified Data.Map as DM
import Data.Text
import Data.Typeable
import qualified Data.ByteString.Lazy as BS

import Database.Graph.HGraphStorage.Types

data Graph = Graph [GraphObject] [GraphRelation]
  deriving (Show,Read,Eq,Ord,Typeable)

data GraphObject = GraphObject
  { goID         :: Maybe ObjectID
  , goType       :: Text
  , goProperties :: DM.Map Text [PropertyValue]
  } deriving (Show,Read,Eq,Ord,Typeable)
  
data GraphRelation = GraphRelation
  { grId         :: Maybe RelationID
  , grFrom       :: GraphObject
  , grTo         :: GraphObject
  , grType       :: Text
  , grProperties :: DM.Map Text [PropertyValue]
  } deriving (Show,Read,Eq,Ord,Typeable)
  

 