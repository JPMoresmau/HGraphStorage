{-# LANGUAGE RankNTypes, OverloadedStrings, PatternGuards, ScopedTypeVariables #-}
module Database.Graph.HGraphStorage.HackageTest where

import Control.Applicative
import qualified Codec.Archive.Tar as Tar
import qualified Codec.Compression.GZip as GZip
import qualified Data.ByteString.Lazy as BS
import System.Directory
import System.FilePath
import Control.Monad (filterM, foldM, when)
import qualified Data.Map.Strict as DM
import qualified Data.Text as T
import Data.Text.Binary
import Distribution.Package
import Distribution.PackageDescription
import Distribution.PackageDescription.Parse
import Distribution.Verbosity
import Distribution.PackageDescription.Configuration (flattenPackageDescription)
import Control.Monad.Logger
import qualified Control.Monad.Trans.Resource as R
import Data.Binary


import Database.Graph.HGraphStorage.API
import Database.Graph.HGraphStorage.Types

buildHackageGraph :: IO(DM.Map T.Text (DM.Map T.Text [(T.Text,T.Text)]))
buildHackageGraph = do
    let serf= "data" </> "index.bin"
    ex <- doesFileExist serf
    if ex
      then decode <$> BS.readFile serf
      else do
        let f= "data" </> "index.tar.gz"
        tmp <- getTemporaryDirectory
        let fldr=tmp </> "hackage-bench"
        createDirectoryIfMissing True fldr
        unTarGzip f fldr
        memG <- createMemoryGraph fldr
        BS.writeFile serf $ encode memG
        return memG

convertHack :: IO ()
convertHack = do
  let serf= "data" </> "index.ser"
  m :: (DM.Map T.Text (DM.Map T.Text [Dependency])) <- read <$> readFile serf
  let m2=DM.map toText1 m
  let binf= "data" </> "index.bin"
  BS.writeFile binf $ encode m2
  where 
    toText1 byVer = DM.map (map toText2) byVer 
    toText2 (Dependency (PackageName name) range)=(T.pack name,T.pack $ show range)

--hackageTest :: IO()
--hackageTest = do
--    let f= "data" </> "index.tar.gz"
--    tmp <- getTemporaryDirectory
--    let fldr=tmp </> "hackage-bench"
--    createDirectoryIfMissing True fldr
--    unTarGzip f fldr
--    memGraph <- createMemoryGraph fldr
--    writeGraph memGraph
--    return ()
                      
-- |Un-gzip and un-tar a file into a folder.
unTarGzip :: FilePath -> FilePath -> IO ()
unTarGzip res folder = do
  cnts <- BS.readFile res
  let ungzip  = GZip.decompress cnts
      entries = Tar.read ungzip
  createDirectories entries
  Tar.unpack folder entries
  where createDirectories Tar.Done     = return ()
        createDirectories (Tar.Fail _) = return ()
        createDirectories (Tar.Next e es) =
            case Tar.entryContent e of
              Tar.NormalFile _ _ -> do let dir = folder </> takeDirectory (Tar.entryPath e)
                                       createDirectoryIfMissing True dir
                                       createDirectories es
              Tar.Directory      -> do let dir = folder </> Tar.entryPath e
                                       createDirectoryIfMissing True dir
                                       createDirectories es
              _                  -> createDirectories es


createMemoryGraph :: FilePath
                       -> IO
                            (DM.Map T.Text (DM.Map T.Text [(T.Text,T.Text)]))
createMemoryGraph folder = do
  cnts <- getSubDirs folder
  foldM processPackage DM.empty cnts
  where
    processPackage m d = do
      putStrLn d
      vrss <- getSubDirs $ folder </> d
      depsByVersion <- foldM (processVersion d) DM.empty vrss
      return $ DM.insert (T.pack d) depsByVersion m
    processVersion d m vr = do
      let cf= addExtension d "cabal" 
      let fullF= folder </> d </> vr </> cf
      ex <- doesFileExist fullF
      if ex
        then do
          putStrLn $ "\t"++vr
          gpd <- readPackageDescription normal fullF
          let pd=flattenPackageDescription gpd
          let mlb=library pd
          case mlb of
            Just lib -> do
              let deps = targetBuildDepends $ libBuildInfo lib
              let depText= map toText deps
              return $ DM.insert (T.pack vr) depText m
            Nothing ->return m
        else return m
    toText (Dependency (PackageName name) range)=(T.pack name,T.pack $ show range)

getSubDirs folder = do
  cnts <- getDirectoryContents folder
  filterM isDir cnts
  where 
    isDir d = 
      if d `notElem` [".",".."]
          then doesDirectoryExist $ folder </> d
          else return False


writeGraph :: DM.Map T.Text (DM.Map T.Text [(T.Text,T.Text)]) -> IO ()
writeGraph memGraph = withTempDB "hackage-test-graph" $ do
  pkgMap <- foldM createPackage DM.empty $ DM.keys memGraph
  mapM_ (createVersions pkgMap) $ DM.toList memGraph
  return ()
  where
    createPackage m pkg = do
      goPkg <- createObject $ GraphObject Nothing "Package" $ DM.fromList [("name",[PVText pkg])]
      return $ DM.insert pkg goPkg m
    createVersions pkgMap (pkg,depsByVersion) 
      | Just goPkg <- DM.lookup pkg pkgMap = do
        mapM_ (createVersion goPkg pkgMap) $ DM.toList depsByVersion
      | otherwise = return ()
    createVersion goPkg pkgMap (vr,deps) = do
      goVer <- createObject $ GraphObject Nothing "Version" $ DM.fromList [("name",[PVText vr])]
      _ <- createRelation' $ GraphRelation Nothing goPkg goVer "versions" DM.empty
      mapM_ (createDep goVer pkgMap) deps
    createDep goVer pkgMap (name,range) 
      | Just goPkg <- DM.lookup name pkgMap = do 
        _ <- createRelation' $ GraphRelation Nothing goVer goPkg "depends" $ DM.fromList [("range",[PVText range])]
        return ()
      | otherwise = return ()
    

withTempDB :: forall b.
  FilePath ->
                GraphStorageT (R.ResourceT (LoggingT IO)) b
                -> IO b
withTempDB fn f = do
  tmp <- getTemporaryDirectory
  let dir = tmp </> fn
  ex <- doesDirectoryExist dir
  when ex $ do
    cnts <- getDirectoryContents dir
    mapM_ removeFile =<< filterM doesFileExist (map (dir </>) cnts)
  runStderrLoggingT $ withGraphStorage dir f