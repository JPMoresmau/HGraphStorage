module Main where
import Database.Graph.HGraphStorage.APITest
import Database.Graph.HGraphStorage.FreeListTest
import Database.Graph.HGraphStorage.QueryTest
import Database.Graph.HGraphStorage.IndexTest

import Test.Tasty


main :: IO()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Graph Storage Tests" [freeListTests, indexTests, apiTests,queryTests]
