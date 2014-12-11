{-# LANGUAGE OverloadedStrings #-}
-- | Test API access
module Database.Graph.HGraphStorage.APITest where

import Test.Tasty
import Control.Monad.IO.Class (liftIO)
import Test.Tasty.HUnit


import qualified Data.Map as DM


import Database.Graph.HGraphStorage.API
import Database.Graph.HGraphStorage.FileOps
import Database.Graph.HGraphStorage.Types
import Database.Graph.HGraphStorage.Utils
import Data.Maybe (isJust)

import qualified Control.Monad.Trans.Resource as R

import Control.Monad.Logger

apiTests :: TestTree
apiTests = testGroup "API tests"
  [ testCase "Create objects" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        objs <- filterObjects (return . const True)
        liftIO $ do
          isJust (goID th) @? "th has no ID!"
          isJust (goID fg) @? "fg has no ID!"
          2 @=? length objs
          (th `elem` objs) @? "th not in list!"
          (fg `elem` objs) @? "fg not in list!"
        return ()
   , testCase "Create relations" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        fgp <- createRelation (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
        ssp <- createRelation (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        rels <- filterRelations (return . const True)
        liftIO $ do
          isJust (grID fgp) @? "fgp has no ID!"
          isJust (grID ssp) @? "ssp has no ID!"
          2 @=? length rels
          (fgp `elem` rels) @? "fgp not in list!"
          (ssp `elem` rels) @? "ssp not in list!"
    , testCase "Check runtime Model" $
        checkModel getModel
    , testCase "Check saved Model" $
        checkModel (readModel =<< getHandles)    
   ]
   
checkModel :: GraphStorageT
                  (R.ResourceT
                     (LoggingT IO))
                  Model
                -> IO ()
checkModel r =
  withTempDB $ do
    th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
    fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
    _ <- createRelation (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
    mdl2 <- r 
    let propL k = DM.lookup k $ fromName $ mPropertyTypes mdl2
    let typeL k = DM.lookup k $ fromName $ mObjectTypes mdl2
    let relL k = DM.lookup k $ fromName $ mRelationTypes mdl2
    liftIO $ do
      isJust (propL ("name",DTText)) @? "no name property type!"
      isJust (propL ("age",DTInteger)) @? "no age property type!"
      isJust (propL ("year",DTInteger)) @? "no year property type!"
      isJust (propL ("role",DTText)) @? "no role property type!"
      isJust (typeL "Actor") @? "no actor object type!"
      isJust (typeL "Movie") @? "no movie object type!"
      isJust (relL "Played") @? "no played relation type!"

