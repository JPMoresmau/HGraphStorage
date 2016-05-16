{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
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

import Control.Monad.Trans.State.Strict

import Control.Applicative
import Control.Monad (MonadPlus)
import Control.Monad.Base (MonadBase(..))
import Control.Monad.Fix (MonadFix)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Class (MonadTrans(lift))
import Control.Monad.Trans.Control ( MonadTransControl(..), MonadBaseControl(..)
                                   , ComposeSt, defaultLiftBaseWith
                                   , defaultRestoreM )
import Control.Monad.Logger
import qualified Control.Monad.Trans.Resource as R

import Data.Default
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BS
import qualified Data.Aeson as A
import Data.Typeable
import qualified Data.Set as S


-- | Our monad transformer.
newtype STMGraphT m a = Gs { unIs :: StateT Database m a }
    deriving ( Functor, Applicative, Alternative, Monad
             , MonadFix, MonadPlus, MonadIO, MonadTrans
             , R.MonadThrow )
-- | Monad Resource instance.
deriving instance R.MonadResource m => R.MonadResource (STMGraphT m)

-- | Monad Base instance.
instance MonadBase b m => MonadBase b (STMGraphT m) where
    liftBase = lift . liftBase

-- | Monad Trans Control instance.
instance MonadTransControl STMGraphT where
    type StT STMGraphT a = StT (StateT Database) a
    liftWith f = Gs $ liftWith (\run -> f (run . unIs))
    restoreT = Gs . restoreT

-- | Monad Base Control instance.
instance MonadBaseControl b m => MonadBaseControl b (STMGraphT m) where
    type StM (STMGraphT m) a = ComposeSt STMGraphT m a
    liftBaseWith = defaultLiftBaseWith
    restoreM = defaultRestoreM

-- | MonadLogger instance.
instance (MonadLogger m) => MonadLogger (STMGraphT m) where
   monadLoggerLog loc src lvl msg=lift $ monadLoggerLog loc src lvl msg

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
getCleaned CleanBoth _ _ = def

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
  | OutE [T.Text]
  | InE [T.Text]
  | BothE [T.Text]
  deriving (Show,Read,Eq,Typeable)

instance Default Traversal where
    def = Noop

instance Monoid Traversal where
  Noop `mappend` b = b
  a `mappend` Noop = a
  a `mappend` b = Composed [a,b]
  mempty = Noop
  mconcat = Composed

data TState
  = SAllNodes
  | SNodes [(NodeID,Node)]
  | SAllEdges
  | SEdges [(EdgeID,Edge)]
  | SProperties [T.Text] [Info]
  | SUnknown
  | SEmpty
  | SError T.Text
  deriving (Show,Read,Eq,Typeable)

instance Monoid TState where
  SEmpty `mappend` b = b
  a `mappend` SEmpty = a
  SUnknown `mappend` b = b
  a `mappend` SUnknown = a
  SError e `mappend` _ = SError e
  _ `mappend` SError e = SError e
  SAllNodes `mappend` SAllNodes = SAllNodes
  SAllNodes `mappend` (SNodes _) = SAllNodes
  SNodes a `mappend` SNodes b = SNodes (a `mappend` b)
  SAllEdges `mappend` SAllEdges = SAllEdges
  SAllEdges `mappend` (SEdges _) = SAllEdges
  SEdges a `mappend` SEdges b = SEdges (a `mappend` b)
  SProperties t1 i1 `mappend` SProperties t2 i2 = SProperties (ordNub $ t1 `mappend` t2) (i1 `mappend` i2)
  a `mappend` b = SError $ T.pack $ "Unsupported State mappend between " ++ show a ++ " and " ++ show b
  mempty = SEmpty

data Result
  = AllNodes
  | Nodes [NodeID]
  | AllEdges
  | Edges [EdgeID]
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
  Error e `mappend` _ = Error e
  _ `mappend` Error e = Error e
  AllNodes `mappend` AllNodes = AllNodes
  AllNodes `mappend` (Nodes _) = AllNodes
  Nodes a `mappend` Nodes b = Nodes (a `mappend` b)
  AllEdges `mappend` AllEdges = AllEdges
  AllEdges `mappend` (Edges _) = AllEdges
  Edges a `mappend` Edges b = Edges (a `mappend` b)
  Properties t1 i1 `mappend` Properties t2 i2 = Properties (ordNub $ t1 `mappend` t2) (i1 `mappend` i2)
  a `mappend` b = Error $ T.pack $ "Unsupported mappend between " ++ show a ++ " and " ++ show b
  mempty = Empty

stateToResult :: TState -> Result
stateToResult SAllNodes = AllNodes
stateToResult (SNodes ns) = Nodes $ map fst ns
stateToResult SAllEdges = AllEdges
stateToResult (SEdges es) = Edges $ map fst es
stateToResult (SProperties t is) = Properties t is
stateToResult SUnknown = Unknown
stateToResult (SError e) = Error e
stateToResult SEmpty = Empty

ordNub :: (Ord a) => [a] -> [a]
ordNub = go S.empty
   where
       go _ []     = []
       go s (x:xs) = if x `S.member` s then go s xs
                                     else x : go (S.insert x s) xs
