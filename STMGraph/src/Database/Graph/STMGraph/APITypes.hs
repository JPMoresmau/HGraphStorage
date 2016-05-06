-----------------------------------------------------------------------------
--
-- Module      :  Database.Graph.STMGraph.APITypes
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

module Database.Graph.STMGraph.APITypes where

import Database.Graph.STMGraph.Types
import Database.Graph.STMGraph.Raw

import Control.Monad
import Control.Monad.STM


import Data.Default
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BS
import qualified Data.Aeson as A
import Data.Typeable


data NameValue
    = TextP {name::T.Text,tValue:: T.Text}
    |   IntP {name::T.Text,iValue:: Integer}
    |   BinP {name::T.Text,bValue:: BS.ByteString}
    |   JsonP {name::T.Text,jValue:: A.Value}
    deriving (Read,Show,Eq,Typeable)

data Info
    = NodeInfo {nodeID :: NodeID, nodeType::T.Text,properties::[NameValue]}
    |  EdgeInfo {edgeID:: EdgeID, edgeType :: T.Text, properties::[NameValue]}
    deriving (Show,Read,Eq,Typeable)

data Traversal
  = Composed [Traversal]
  | Ns
  | Es
  | NID [NodeID]
  | EID [EdgeID]
  | Has NameValue
  | Values [T.Text]
  | AllValues
  | Noop
  deriving (Show,Read,Eq,Typeable)

instance Monoid Traversal where
  Noop `mappend` b = b
  a `mappend` Noop = a
  a `mappend` b = Composed [a,b]
  mempty = Noop
  mconcat = Composed

data Result
  = AllNodes
  | Nodes [(NodeID,Node)]
  | AllEdges
  | Edges [(EdgeID,Edge)]
  | Properties [T.Text] [Info]
  | Unknown
  | Empty
  | Error T.Text
  deriving (Show,Read,Eq,Typeable)

