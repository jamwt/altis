module Database.Altis.Storage where

import Control.Concurrent.STM (STM, atomically)
import Control.Concurrent.STM.TVar (TVar, writeTVar, readTVar)

import qualified Data.Map as M
import qualified Data.ByteString.Char8 as S

type AltisKey = S.ByteString 
data AltisValue = AltisString S.ByteString 
    deriving (Show)
type AltisDataset = TVar (M.Map AltisKey AltisValue)

fetch :: AltisDataset -> AltisKey -> IO (Maybe AltisValue)
fetch db key = atomically $ do
    mp <- readTVar db
    return $! M.lookup key mp

type Modifier = Maybe AltisValue -> ModifyResult

data ModifyResult = ModifyOkay (Maybe AltisValue)
                  | ModifyFail S.ByteString -- why?

modify :: AltisDataset -> AltisKey -> Modifier -> IO ModifyResult
modify db key mod = atomically $ do
    mp <- readTVar db
    let !mv = M.lookup key mp
    let modres  = mod mv
    case modres of
        ModifyOkay (Just !new) -> do
            let !added = M.insert key new mp
            writeTVar db added
        ModifyOkay Nothing    -> do
            let !removed = M.delete key mp
            writeTVar db removed
        ModifyFail _          -> return ()
    return $ modres
