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
    = TextP T.Text T.Text
    |   IntP T.Text Integer
    |   BinP T.Text BS.ByteString
    |   JsonP T.Text A.Value
    deriving (Read,Show,Eq,Typeable)


data Traversal
  = Composed [Traversal]
  | Ns
  | Es
  | NID [NodeID]
  | EID [EdgeID]
  | Has NameValue
  | Values [T.Text]
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
  | Properties [[NameValue]]
  | Unknown
  | Empty
  | Error T.Text
  deriving (Show,Read,Eq,Typeable)

