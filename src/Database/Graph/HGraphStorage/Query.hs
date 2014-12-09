{-# LANGUAGE DeriveDataTypeable, ConstraintKinds, FlexibleContexts #-}
-- | Higher level API for querying
module Database.Graph.HGraphStorage.Query where

import Data.Default
import Data.Typeable
import qualified Data.Text as T
import qualified Data.Map as DM

import Database.Graph.HGraphStorage.API
import Database.Graph.HGraphStorage.FileOps
import Database.Graph.HGraphStorage.Types


data RelationDir = OUT | IN | BOTH
  deriving (Show,Read,Eq,Ord,Bounded,Enum,Typeable)
  
data RelationStep = RelationStep
  { rsRelTypes  :: [T.Text]
  , rsDirection :: RelationDir
  , rsTgtTypes  :: [T.Text]
  , rsTgtFilter :: GraphObject -> Bool
  } deriving (Typeable)

instance Default RelationStep where
  def = RelationStep [] OUT [] (const True)

data StepResult = StepResult
  { srDirection :: RelationDir
  , srType       :: T.Text
  , srProperties :: DM.Map T.Text [PropertyValue]
  , srObject     :: GraphObject
  } deriving (Show,Read,Eq,Ord,Typeable)

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
      then filt1 (oFirstFrom o) rFromNext rToType rTo OUT []
      else return []
  if rsDirection rs `elem` [IN,BOTH]
      then filt1 (oFirstTo o) rToNext rFromType rFrom IN froms
      else return froms
  where
    isRestricted [] _ =True
    isRestricted ls l = l `elem` ls
    filterRels hs resRels resObjs filt fid tonext tgtType tgtId dir accum 
      | fid == def = return accum
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
                return $ StepResult dir typeName pmap obj : accum
              else return accum
          else return accum
        filterRels hs resRels resObjs filt next tonext tgtType tgtId dir accum2
