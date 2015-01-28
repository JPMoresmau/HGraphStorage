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
import Database.Graph.HGraphStorage.Index


apiTests :: TestTree
apiTests = testGroup "API tests"
  [ testCase "Create objects" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        objs <- filterObjects (return . const True)
        liftIO $ do
          2 @=? length objs
          (th `elem` objs) @? "th not in list!"
          (fg `elem` objs) @? "fg not in list!"
        return ()
   , testCase "Create relations" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        fgp <- createRelation' (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
        ssp <- createRelation' (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        rels <- filterRelations (return . const True)
        liftIO $ do
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
        _ <- createRelation' (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
        _ <- createRelation' (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        deleteObject $ goID th
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
        fgp <- createRelation' (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
        ssp <- createRelation' (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        deleteRelation $ grID ssp
        qs1 <- queryStep (goID th) def
        let sr1_1 = StepResult (grID fgp) OUT "Played" (DM.fromList [("role",[PVText "Forrest Gump"])]) fg
        qs2 <- queryStep (goID th) def{rsDirection = IN}
        qs3 <- queryStep (goID ss) def{rsDirection = IN}
        qs4 <- queryStep (goID fg) def
        qs5 <- queryStep (goID fg) def{rsDirection = IN}
        let sr5_1 = StepResult (grID fgp) IN "Played" (DM.fromList [("role",[PVText "Forrest Gump"])]) th
        deleteRelation $ grID fgp
        qs6 <- queryStep (goID th) def
        qs7 <- queryStep (goID fg) def{rsDirection = IN}
        fgp2 <- createRelation' (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ssp2 <- createRelation' (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        deleteRelation $ grID fgp2
        qs8 <- queryStep (goID th) def
        let sr8_1 = StepResult (grID ssp2) OUT "Played" (DM.fromList [("role",[PVText "Sam Baldwin"])]) ss
        
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
        deleteObject $ goID th
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        liftIO $
          goID th @=? goID fg
   , testCase "Edit objects" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        let nProps= DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 61]),("hair",[PVText "curly"])]
        th2 <- updateObject th{goProperties=nProps}
        th3 <- getObject $ goID th2
        let nProps2= DM.fromList [("name",[PVText "Tom Hanks"]),("hair",[PVText "curly"])]
        th4 <- updateObject th3{goProperties=nProps2}
        th5 <- getObject $ goID th3
        liftIO $ do
          goID th @=? goID th2
          goID th @=? goID th3
          nProps @=? goProperties th3
          goID th @=? goID th4
          goID th @=? goID th5
          nProps2 @=? goProperties th5
   , testCase "Indexing one object" $
      withTempDB $ do
        tr<-addIndex $ IndexInfo "LastName" ["Actor"] ["lastName"]
        allIdx0 <- liftIO $ prefix [] tr
        th0 <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("firstName",[PVText "Tom"]),("lastName",[PVText "Hanks"])])
        allIdx1 <- liftIO $ prefix [] tr
        _ <- updateObject (GraphObject (goID th0) "Actor" $ DM.fromList [("firstName",[PVText "Tom"]),("lastName",[PVText "Cruise"])])
        allIdx2 <- liftIO $ prefix [] tr
        deleteObject $ goID th0
        allIdx3 <- liftIO $ prefix [] tr
        liftIO $ do
          allIdx0 @?= []
          allIdx1 @?= [(textToKey "Hanks",goID th0)]
          allIdx2 @?= [(textToKey "Cruise",goID th0)]
          allIdx3 @?= []
   , testCase "Indexing two objects" $
      withTempDB $ do
        tr<-addIndex $ IndexInfo "LastName" ["Actor"] ["lastName"]
        allIdx0 <- liftIO $ prefix [] tr
        th0 <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("firstName",[PVText "Tom"]),("lastName",[PVText "Hanks"])])
        allIdx1 <- liftIO $ prefix [] tr
        th1 <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("firstName",[PVText "Tom"]),("lastName",[PVText "Cruise"])])
        allIdx2 <- liftIO $ prefix [] tr
        deleteObject $ goID th0
        allIdx3 <- liftIO $ prefix [] tr
        liftIO $ do
          allIdx0 @?= []
          allIdx1 @?= [(textToKey "Hanks",goID th0)]
          allIdx2 @?= [(textToKey "Hanks",goID th0),(textToKey "Cruise",goID th1)]       
          allIdx3 @?= [(textToKey "Cruise",goID th1)]
   , testCase "Create index after objects" $
      withTempDB $ do
       th0 <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("firstName",[PVText "Tom"]),("lastName",[PVText "Hanks"])])
       th1 <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("firstName",[PVText "Tom"]),("lastName",[PVText "Cruise"])])
       tr<-addIndex $ IndexInfo "LastName" ["Actor"] ["lastName"] 
       allIdx0 <- liftIO $ prefix [] tr
       liftIO $
          allIdx0 @?= [(textToKey "Hanks",goID th0),(textToKey "Cruise",goID th1)]       
    , testCase "Index persistence" $ do
       let ii = IndexInfo "LastName" ["Actor"] ["lastName"]
       dir <- withTempDB $ do
                _<- addIndex ii
                getDirectory
       runStderrLoggingT $ withGraphStorage dir def $ do
        idxs <- getIndices
        liftIO $ 
          [ii] @=? map fst idxs
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
    _ <- createRelation' (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
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

