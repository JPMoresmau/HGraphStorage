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
import Database.Graph.HGraphStorage.Query
import Database.Graph.HGraphStorage.Utils
import Data.Maybe

import qualified Control.Monad.Trans.Resource as R

import Control.Monad.Logger
import Data.Default (def)


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
    , testCase "Delete objects" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        _ <- createRelation (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
        _ <- createRelation (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        deleteObject $ fromJust $ goID th
        objs <- filterObjects (return . const True)
        rels <- filterRelations (return . const True)
        liftIO $ do
          2 @=? length objs
          (th `notElem` objs) @? "th in list!"
          0 @=? length rels
    , testCase "Delete relations" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        fgp <- createRelation (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
        ssp <- createRelation (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        deleteRelation $ fromJust $ grID ssp
        qs1 <- queryStep (fromJust $ goID th) def
        let sr1_1 = StepResult (fromJust $ grID fgp) OUT "Played" (DM.fromList [("role",[PVText "Forrest Gump"])]) fg
        qs2 <- queryStep (fromJust $ goID th) def{rsDirection = IN}
        qs3 <- queryStep (fromJust $ goID ss) def{rsDirection = IN}
        qs4 <- queryStep (fromJust $ goID fg) def
        qs5 <- queryStep (fromJust $ goID fg) def{rsDirection = IN}
        let sr5_1 = StepResult (fromJust $ grID fgp) IN "Played" (DM.fromList [("role",[PVText "Forrest Gump"])]) th
        deleteRelation $ fromJust $ grID fgp
        qs6 <- queryStep (fromJust $ goID th) def
        qs7 <- queryStep (fromJust $ goID fg) def{rsDirection = IN}
        fgp2 <- createRelation (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ssp2 <- createRelation (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        deleteRelation $ fromJust $ grID fgp2
        qs8 <- queryStep (fromJust $ goID th) def
        let sr8_1 = StepResult (fromJust $ grID ssp2) OUT "Played" (DM.fromList [("role",[PVText "Sam Baldwin"])]) ss
        
        liftIO $ do
          1 @=? length qs1
          (sr1_1 `elem` qs1) @? "sr1_1 not in out list!"
          0 @=? length qs2
          0 @=? length qs3
          0 @=? length qs4
          1 @=? length qs5
          (sr5_1 `elem` qs5) @? "sr5_1 not in in list!"
          0 @=? length qs6
          0 @=? length qs7
          1 @=? length qs8
          (sr8_1 `elem` qs8) @? "sr8_1 not in in list!"
   , testCase "Delete objects recovers id" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        deleteObject $ fromJust $ goID th
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        liftIO $
          goID th @=? goID fg
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

