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
import qualified Data.Set as S

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

data EdgeRemoval
  = CleanFrom
  | CleanTo
  | CleanBoth

shouldCleanFrom :: EdgeRemoval -> Bool
shouldCleanFrom CleanTo = False
shouldCleanFrom _ = True

shouldCleanTo :: EdgeRemoval -> Bool
shouldCleanTo CleanFrom = False
shouldCleanTo _ = True

getCleaned :: Default a => EdgeRemoval -> a -> a -> a
getCleaned CleanFrom _ to = to
getCleaned CleanTo from _ = from
getCleant CleanBoth _ _ = def

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
  | Out [T.Text]
  | In [T.Text]
  | Both [T.Text]
  deriving (Show,Read,Eq,Typeable)

instance Default Traversal where
    def = Noop

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

instance Monoid Result where
  Empty `mappend` b = b
  a `mappend` Empty = a
  Unknown `mappend` b = b
  a `mappend` Unknown = a
  Error e `mappend` b = Error e
  a `mappend` Error e = Error e
  AllNodes `mappend` AllNodes = AllNodes
  AllNodes `mappend` (Nodes _) = AllNodes
  Nodes a `mappend` Nodes b = Nodes (a `mappend` b)
  AllEdges `mappend` AllEdges = AllEdges
  AllEdges `mappend` (Edges _) = AllEdges
  Edges a `mappend` Edges b = Edges (a `mappend` b)
  Properties t1 i1 `mappend` Properties t2 i2 = Properties (ordNub $ t1 `mappend` t2) (i1 `mappend` i2)
  a `mappend` b = error $ "unsupported mappend between " ++ show a ++ " and " ++ show b
  mempty = Empty


ordNub :: (Ord a) => [a] -> [a]
ordNub = go S.empty
   where
       go _ []     = []
       go s (x:xs) = if x `S.member` s then go s xs
                                     else x : go (S.insert x s) xs
