{-# LANGUAGE RankNTypes, OverloadedStrings, PatternGuards, ScopedTypeVariables #-}
module Database.Graph.STMGraph.HackageTest where

import Control.Applicative
import qualified Codec.Archive.Tar as Tar
import qualified Codec.Compression.GZip as GZip
import qualified Data.ByteString.Lazy as BS
import System.Directory
import System.FilePath
import Control.Monad (filterM, foldM, when)
import qualified Data.Map.Strict as DM
import qualified Data.Text as T
import Data.Text.Binary ()
import Distribution.Package hiding (depends)
import Distribution.PackageDescription
import Distribution.PackageDescription.Parse
import Distribution.Verbosity
import Distribution.PackageDescription.Configuration (flattenPackageDescription)
import Control.Monad.Logger
import qualified Control.Monad.Trans.Resource as R
import Data.Binary

import Control.Monad.STM
import Database.Graph.STMGraph.API
import Control.Monad.IO.Class (liftIO)
import Data.Default (def)

import Network.HTTP.Client
import Control.Exception
import Debug.Trace

import           System.IO

type GraphMap = DM.Map T.Text (DM.Map T.Text [(T.Text,T.Text)])

buildHackageGraph :: IO GraphMap
buildHackageGraph = do
    let serf= "data" </> "index.bin"
    ex <- doesFileExist serf
    if ex
      then decode <$> BS.readFile serf
      else do
        let f= "data" </> "index.tar.gz"
        exgz <- doesFileExist f
        when (not exgz) $ do
            manager <- newManager defaultManagerSettings
            request <- parseUrl "http://hackage.haskell.org/packages/index.tar.gz"
            response <- httpLbs request manager
            BS.writeFile f $ responseBody response
        tmp <- getTemporaryDirectory
        let fldr=tmp </> "hackage-bench"
        createDirectoryIfMissing True fldr
        unTarGzip f fldr
        memG <- createMemoryGraph fldr
        BS.writeFile serf $ encode memG
        return memG

-- code to convert serialize to binary
--convertHack :: IO ()
--convertHack = do
--  let serf= "data" </> "index.ser"
--  m :: (DM.Map T.Text (DM.Map T.Text [Dependency])) <- read <$> readFile serf
--  let m2=DM.map toText1 m
--  let binf= "data" </> "index.bin"
--  BS.writeFile binf $ encode m2
--  where
--    toText1 byVer = DM.map (map toText2) byVer
--    toText2 (Dependency (PackageName name) range)=(T.pack name,T.pack $ show range)


-- | Un-gzip and un-tar a file into a folder.
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
                       -> IO GraphMap
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

getSubDirs :: FilePath -> IO [FilePath]
getSubDirs folder = do
  cnts <- getDirectoryContents folder
  filterM isDir cnts
  where
    isDir d =
      if d `notElem` [".",".."]
          then doesDirectoryExist $ folder </> d
          else return False


writeGraph :: GraphMap -> IO Int
writeGraph memGraph = withTempDB "hackage-test-stmgraph" True $ do
  --indexPackageNames <- createIndex "packageNames"
  -- _ <- addIndex $ IndexInfo "packageNames" ["Package"] ["name"]
  pkgMap <- foldM createPackage DM.empty $ take 2000 $ DM.keys memGraph
  mapM_ (createVersions pkgMap) $ DM.toList memGraph
  nbNodes
  where
    createPackage m pkg = do
      goPkg <-addNode "Package" [TextP "name" pkg]
      --liftIO $ putStrLn $ (T.unpack pkg) ++"->" ++ (show $ textToKey pkg) ++ ":" ++ (show $ goID goPkg)
      -- ml <- liftIO $ Idx.lookup key indexPackageNames
      -- when (Just (goID goPkg) /= ml) $ error $ "wrong lookup: "++ (show key) ++ "->" ++ show ml
      return $ DM.insert pkg goPkg m
    createVersions pkgMap (pkg,depsByVersion)
      | Just goPkg <- DM.lookup pkg pkgMap = mapM_ (createVersion goPkg pkgMap) $ DM.toList depsByVersion
      | otherwise = return ()
    createVersion goPkg pkgMap (vr,deps) = do
      goVer <- addNode "Version" [TextP "name" vr]
      _ <- addEdge goPkg versions [] goVer
      mapM_ (createDep goVer pkgMap) deps
    createDep goVer pkgMap (name,range)
      | Just goPkg <- DM.lookup name pkgMap = do
        _ <- addEdge goVer depends [TextP "range" range] goPkg
        return ()
      | otherwise = return ()

--
--nameIndex :: DM.Map T.Text (DM.Map T.Text [(T.Text,T.Text)]) -> IO ()
--nameIndex memGraph = withTempDB "hackage-test-graph" False def $ do
--  indexPackageNames <- (snd . head) <$> getIndices
--  -- :: Trie Int16 ObjectID <- createIndex "packageNames"
--  let ks = DM.keys memGraph
--  let pkgLen = length ks
--  pkgMap <- foldM (getPackage indexPackageNames) DM.empty ks
--  let idLen = length $ DM.keys pkgMap
--  when (pkgLen /= idLen) $ error $ "wrong length:" ++ show idLen
--  return ()
--  where
--    getPackage indexPackageNames m pkg = do
--      mid <- liftIO $ Idx.lookup (textToKey pkg) indexPackageNames
--      return $ case mid of
--        Just oid -> DM.insert oid pkg m
--        _ -> error $ "empty lookup:" ++ T.unpack pkg
--
--yesodQuery :: IO Int
--yesodQuery  = withTempDB "hackage-test-graph" False def $ do
--  indexPackageNames <- (snd . head) <$> getIndices
--  -- :: Trie Int16 ObjectID <- createIndex "packageNames"
--  mid <- liftIO $ Idx.lookup (textToKey "yesod") indexPackageNames
--  case mid of
--    Just oid -> do
--      res <- queryStep oid def{rsRelTypes=[versions],rsDirection = OUT}
--      let l = length res
--      when (l < 111) $ error "less than 111 versions for yesod"
--      return l
--    _ -> error "empty lookup for yesod"


versions :: T.Text
versions = "versions"

depends :: T.Text
depends = "depends"

withTempDB :: forall b.
  FilePath -> Bool -> (STMGraphT (R.ResourceT IO) b)
                -> IO b
withTempDB fn del f = do
  tmp <- getTemporaryDirectory
  let dir = tmp </> fn
  ex <- doesDirectoryExist dir
  when (ex && del) $ do
    cnts <- getDirectoryContents dir
    mapM_ removeFile =<< filterM doesFileExist (map (dir </>) cnts)
  withDatabaseIO dir def f
--{gsMainBuffering=Just $ BlockBuffering $ Just 4096}
