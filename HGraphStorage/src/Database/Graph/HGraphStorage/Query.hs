{-# LANGUAGE DeriveDataTypeable, ConstraintKinds, FlexibleContexts, PatternGuards #-}
-- | Higher level API for querying
module Database.Graph.HGraphStorage.Query where

import Control.Applicative
import Data.Default
import Data.Typeable
import qualified Data.Text as T
import qualified Data.Map as DM

import Database.Graph.HGraphStorage.API
import Database.Graph.HGraphStorage.FileOps
import Database.Graph.HGraphStorage.Types

-- | Direction to follow
data RelationDir = OUT | IN | BOTH
  deriving (Show,Read,Eq,Ord,Bounded,Enum,Typeable)
  
-- | One step in the query
data RelationStep = RelationStep
  { rsRelTypes  :: [T.Text] -- ^ Types of relations to follow (empty -> all)
  , rsDirection :: RelationDir -- ^ Direction of relation
  , rsTgtTypes  :: [T.Text] -- ^ Types of objects to retrieve (empty -> all)
  , rsTgtFilter :: GraphObject ObjectID -> Bool -- ^ Condition to match on objects
  , rsLimit     :: Maybe Int -- ^ Maximum number of relations to follow (limit applies after all other filters)
  } deriving (Typeable)

-- | Default instance: navigates all out links
instance Default RelationStep where
  def = RelationStep [] OUT [] (const True) Nothing

-- | Result of a query step
data StepResult = StepResult
  { srRelationID :: RelationID -- ^ Relation id
  , srDirection  :: RelationDir -- ^ Direction of relation
  , srType       :: T.Text -- ^ Type of relation
  , srProperties :: DM.Map T.Text [PropertyValue] -- ^ Properties of relation
  , srObject     :: GraphObject ObjectID -- ^ Target object
  } deriving (Show,Read,Eq,Ord,Typeable)


-- | Run a one step query on one given object
queryStep 
  :: (GraphUsableMonad m) 
  => ObjectID -> RelationStep -> GraphStorageT m [StepResult]
queryStep oid rs = do
  hs <- getHandles
  o <- readOne hs oid
  restrictedRelTypes <- mapM relationType $ rsRelTypes rs
  restrictedObjTypes <- mapM objectType $ rsTgtTypes rs
  let filt1 = filterRels hs restrictedRelTypes restrictedObjTypes (rsTgtFilter rs)
  froms <- 
    if rsDirection rs `elem` [OUT,BOTH]
      then filt1 (oFirstFrom o) rFromNext rToType rTo OUT ([],0)
      else return ([],0)
  if rsDirection rs `elem` [IN,BOTH]
      then fst <$> filt1 (oFirstTo o) rToNext rFromType rFrom IN froms 
      else return $ fst froms
  where
    isRestricted [] _ =True
    isRestricted ls l = l `elem` ls
    filterRels hs resRels resObjs filt fid tonext tgtType tgtId dir (accum,cnt) 
      | fid == def = return (accum,cnt)
      | Just a <- rsLimit rs , 
        cnt==a = return (accum,cnt)
      | otherwise  = do
        rel <- readOne hs fid
        let next = tonext rel
        accum2 <- if isRestricted resRels (rType rel) &&  isRestricted resObjs (tgtType rel)
          then do
            let oid2 = tgtId rel
            obj <- getObject oid2
            if filt obj 
              then do
                mdl <- getModel
                let pid = rFirstProperty rel
                pmap <- listProperties pid
                let rtid = rType rel
                typeName <- throwIfNothing (UnknownRelationType rtid) $ DM.lookup rtid $ toName $ mRelationTypes mdl
                return (StepResult fid dir typeName pmap obj : accum,cnt+1)
              else return (accum,cnt)
          else return (accum,cnt)
        filterRels hs resRels resObjs filt next tonext tgtType tgtId dir accum2
