{-# LANGUAGE OverloadedStrings #-}
-- | Test queries
module Database.Graph.HGraphStorage.QueryTest where

import Database.Graph.HGraphStorage.API
import Database.Graph.HGraphStorage.Query
import Database.Graph.HGraphStorage.Utils
import Database.Graph.HGraphStorage.Types

import qualified Data.Map as DM

import Test.Tasty
import Test.Tasty.HUnit
import Data.Default (def)
import Control.Monad.IO.Class (liftIO)

queryTests :: TestTree
queryTests = testGroup "Query tests"
  [ testCase "Single Step" $
      withTempDB $ do
        th <- createObject (GraphObject Nothing "Actor" $ DM.fromList [("name",[PVText "Tom Hanks"]),("age",[PVInteger 60])])
        fg <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Forrest Gump"]),("year",[PVInteger 1990])])
        fgp <- createRelation' (GraphRelation Nothing th fg "Played" $ DM.fromList [("role",[PVText "Forrest Gump"])])
        ss <- createObject (GraphObject Nothing "Movie" $ DM.fromList [("name",[PVText "Sleepless in Seattle"]),("year",[PVInteger 1990])])
        ssp <- createRelation' (GraphRelation Nothing th ss "Played" $ DM.fromList [("role",[PVText "Sam Baldwin"])])
        qs1 <- queryStep (goID th) def
        let sr1_1 = StepResult (grID fgp) OUT "Played" (DM.fromList [("role",[PVText "Forrest Gump"])]) fg
        let sr1_2 = StepResult (grID ssp) OUT "Played" (DM.fromList [("role",[PVText "Sam Baldwin"])]) ss
        qs2 <- queryStep (goID th) def{rsDirection = IN}
        qs3 <- queryStep (goID th) def{rsDirection = BOTH}
        qs4 <- queryStep (goID fg) def
        qs5 <- queryStep (goID fg) def{rsDirection = IN}
        let sr5_1 = StepResult (grID fgp) IN "Played" (DM.fromList [("role",[PVText "Forrest Gump"])]) th
        qs6 <- queryStep (goID fg) def{rsDirection = BOTH}
        liftIO $ do
          2 @=? length qs1
          (sr1_1 `elem` qs1) @? "sr1_1 not in out list!"
          (sr1_2 `elem` qs1) @? "sr1_2 not in out list!"
          0 @=? length qs2
          2 @=? length qs3
          (sr1_1 `elem` qs3) @? "sr1_1 not in both list!"
          (sr1_2 `elem` qs3) @? "sr1_2 not in both list!"
          0 @=? length qs4
          1 @=? length qs5
          (sr5_1 `elem` qs5) @? "sr5_1 not in in list!"
          1 @=? length qs6
          (sr5_1 `elem` qs6) @? "sr5_1 not in both list!"
  ]