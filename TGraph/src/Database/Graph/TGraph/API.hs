module Database.Graph.TGraph.API
     where

import Database.Graph.TGraph.Model
import Database.Graph.TGraph.Types
import Data.TCache
import           Data.Typeable
import           GHC.Generics                           (Generic)
import qualified Data.Text as T
import qualified Data.Map as DM

-- | An object with a type and properties.
data GraphObject a = GraphObject
  { goID         :: a
  , goType       :: T.Text
  , goProperties :: DM.Map PropertyType [PropertyValue]
  } deriving (Show,Read,Eq,Ord,Typeable)

createObject :: GraphObject (Maybe ObjectID) -> DBRef Model -> STM (GraphObject ObjectID)
createObject obj mdl = do
  ot <- getObjectTypeByName mdl (goType obj)
  return undefined
