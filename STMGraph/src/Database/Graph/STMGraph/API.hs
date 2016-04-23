{-# LANGUAGE DeriveDataTypeable #-}
module Database.Graph.STMGraph.API
  ( addVertex
  , NameValue(..)
  )where

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

toPropertyValue :: NameValue -> (T.Text,PropertyValue)
toPropertyValue (TextP n v)=(n,PVText v)
toPropertyValue (IntP n v)=(n,PVInteger v)
toPropertyValue (BinP n v)=(n,PVBinary v)
toPropertyValue (JsonP n v)=(n,PVJSON v)


addVertex :: Database -> T.Text -> [NameValue] -> STM ObjectID
addVertex db tp props = do
  pid <- createProperties db props
  tid <- getObjectTypeID db tp
  let obj=Object tid def def pid
  writeObject db Nothing obj

createProperties :: Database -> [NameValue] -> STM PropertyID
createProperties db = foldM createProperty def . (map toPropertyValue)
  where
    createProperty nid (name,value) = do
      let dt=valueType value
      ptid <- getPropertyTypeID db (name,dt)
      writeProperty db Nothing ((ptid,nid),value)

data Traversal
  = Composed [Traversal]
  | V
  | HasOID ObjectID
  | HasRID RelationID
  | Has NameValue
  | Noop
  deriving (Show,Read,Eq,Typeable)

instance Monoid Traversal where
  Noop `mappend` b = b
  a `mappend` Noop = a
  a `mappend` b = Composed [a,b]
  mempty = Noop
  mconcat = Composed

data Result
 = Vertices [ObjectID]
 | Edges [RelationID]
 | Properties [[NameValue]]
 deriving (Show,Read,Eq,Typeable)
