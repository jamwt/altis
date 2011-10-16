module Database.Altis.Commands where

import qualified Data.ByteString.Char8 as S

import Debug.Trace (trace)
import Database.Altis.Storage

data AltisRequest = AltisRequest S.ByteString [S.ByteString] 
    deriving (Show)

data StringOrNull = StringResponse S.ByteString
                  | NullResponse
    deriving (Show)

data AltisResponse = AltisErrorResponse S.ByteString
                   | AltisLineResponse S.ByteString
                   | AltisIntResponse Integer
                   | AltisBulkResponse StringOrNull
                   | AltisMultiBulkResponse [StringOrNull]
    deriving (Show)

typeError = AltisErrorResponse "operation of wrong type"

command :: AltisDataset -> AltisRequest -> IO AltisResponse
command db (AltisRequest "GET" [key]) = do
    lv <- fetch db key
    return $ case lv of
        Nothing -> AltisBulkResponse NullResponse
        Just (AltisString s) -> AltisBulkResponse $ StringResponse s
        Just _ -> typeError

command db (AltisRequest "SET" (key:value:[])) = do
    result <- modify db key setString
    return $ case result of
        ModifyOkay (Just _) -> AltisLineResponse "OK"
        _                   -> error "SET failed?? should not happen!"
  where
    setString v = ModifyOkay $ Just (AltisString value)

command db req = 
    error (show req)
