{-# LANGUAGE DeriveGeneric #-}
module Database.Graph.TGraph.Model

where

import Control.Exception.Base
import Control.Monad.STM
import Data.Default
import Data.TCache
import Data.List
import Data.Ord
import qualified Data.Text as T
import Data.Typeable
import Database.Graph.TGraph.Types
import           GHC.Generics                           (Generic)


getDefRef :: (Default a,Typeable a,IResource a) => DBRef a -> STM a
getDefRef rmdl = do
  mmdl <- readDBRef rmdl
  case mmdl of
    Just mdl -> return mdl
    Nothing -> do
      let mdl = def
      writeDBRef rmdl mdl
      return mdl

getObjectTypes :: DBRef Model -> STM [ObjectType]
getObjectTypes rmdl = mObjectTypes <$> getDefRef rmdl

getObjectTypeByName :: DBRef Model -> T.Text -> STM ObjectType
getObjectTypeByName rmdl name = do
  mdl <- getDefRef rmdl
  let allOts = mObjectTypes mdl
  let ots = filter (\x->otName x == name) allOts
  case ots of
    [ot] -> return ot
    (_:_) -> throwSTM $ DuplicateObjectTypeName name
    []     -> do
      let m = if null allOts then 0 else otID $ maximumBy (comparing otID) allOts
          ot = ObjectType (m+1) name
      writeDBRef rmdl mdl{mObjectTypes=ot:allOts}
      return ot

getObjectTypeByID :: DBRef Model -> ObjectTypeID -> STM ObjectType
getObjectTypeByID rmdl otid = do
  mdl <- getDefRef rmdl
  let allOts = mObjectTypes mdl
  let ots = filter (\x->otID x == otid) allOts
  case ots of
    [ot] -> return ot
    [] -> throwSTM $ MissingObjectTypeID otid
    _ -> throwSTM $ DuplicateObjectTypeID otid

getRelationTypeByName :: DBRef Model -> T.Text -> STM RelationType
getRelationTypeByName rmdl name = do
  mdl <- getDefRef rmdl
  let allRts = mRelationTypes mdl
  let rts = filter (\x->rtName x == name) allRts
  case rts of
    [rt] -> return rt
    (_:_) -> throwSTM $ DuplicateRelationTypeName name
    []     -> do
      let m = if null allRts then 0 else rtID $ maximumBy (comparing rtID) allRts
          rt = RelationType (m+1) name
      writeDBRef rmdl mdl{mRelationTypes=rt:allRts}
      return rt

getRelationTypeByID :: DBRef Model -> RelationTypeID -> STM RelationType
getRelationTypeByID rmdl rtid = do
  mdl <- getDefRef rmdl
  let allRts = mRelationTypes mdl
  let rts = filter (\x->rtID x == rtid) allRts
  case rts of
    [rt] -> return rt
    [] -> throwSTM $ MissingRelationTypeID rtid
    _ -> throwSTM $ DuplicateRelationTypeID rtid

getPropertyTypeByName :: DBRef Model -> (T.Text,DataType) -> STM PropertyType
getPropertyTypeByName rmdl (name,dt) = do
  mdl <- getDefRef rmdl
  let allPts = mPropertyTypes mdl
  let rts = filter (\x->ptName x == name && ptDataType x == dt) allPts
  case rts of
    [pt] -> return pt
    (_:_) -> throwSTM $ DuplicatePropertyTypeName name
    []     -> do
      let m = if null allPts then 0 else ptID $ maximumBy (comparing ptID) allPts
          pt = PropertyType (m+1) dt name
      writeDBRef rmdl mdl{mPropertyTypes=pt:allPts}
      return pt

getPropertyTypeByID :: DBRef Model -> PropertyTypeID -> STM PropertyType
getPropertyTypeByID rmdl ptid = do
  mdl <- getDefRef rmdl
  let allPts = mPropertyTypes mdl
  let pts = filter (\x->ptID x == ptid) allPts
  case pts of
    [pt] -> return pt
    [] -> throwSTM $ MissingPropertyTypeID ptid
    _ -> throwSTM $ DuplicatePropertyTypeID ptid

data ModelException =
    MissingObjectTypeID ObjectTypeID
  | DuplicateObjectTypeID ObjectTypeID
  | DuplicateObjectTypeName T.Text
  | MissingRelationTypeID RelationTypeID
  | DuplicateRelationTypeID RelationTypeID
  | DuplicateRelationTypeName T.Text
  | MissingPropertyTypeID PropertyTypeID
  | DuplicatePropertyTypeID PropertyTypeID
  | DuplicatePropertyTypeName T.Text
  deriving (Show,Read,Eq,Ord,Typeable,Generic)

instance Exception ModelException
