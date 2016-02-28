{-# LANGUAGE OverloadedStrings #-}
-- | Some constants values
module Database.Graph.TGraph.Constants where

import Data.Text (Text)

modelName :: String
modelName = "model"

maxName :: String
maxName = "maxIDs"

-- | The name of the "name" property
namePropertyName :: Text
namePropertyName = "name"

-- | The name of the object file
objectFile :: FilePath
objectFile = "objects.db"

-- | The name of the object type file
objectTypeFile :: FilePath
objectTypeFile = "objecttypes.db"

-- | The name of the relation file
relationFile :: FilePath
relationFile = "relations.db"

-- | The name of the object type file
relationTypeFile :: FilePath
relationTypeFile = "relationtypes.db"

-- | The name of the property file
propertyFile :: FilePath
propertyFile = "properties.db"

-- | The name of the property type file
propertyTypeFile :: FilePath
propertyTypeFile = "propertytypes.db"

-- | The name of the property value file
propertyValuesFile :: FilePath
propertyValuesFile = "propertyvalues.db"

-- | Prefix for free list files
freePrefix :: FilePath
freePrefix = "free-"

-- | The name of the max ids file
maxIDsFile :: FilePath
maxIDsFile = "maxids.db"
