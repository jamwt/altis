module Database.Altis.ServerProtocol where

import qualified Data.Map as M
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as BinSock
import Network.BSD
import Control.Exception (finally)
import Control.Monad (forever, liftM, replicateM, void)
import Control.Monad.Trans (liftIO)
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM (newTVar, newTVarIO, atomically, readTVar)
import Data.Enumerator (($$), run_)
import qualified Data.Enumerator as E
import Data.Enumerator.Binary (enumHandle)
import Data.Attoparsec.Enumerator (iterParser)
import System.IO (hSetBuffering, hClose, BufferMode(..), 
    IOMode(..), Handle)
import Data.Char (ord, chr)
import Data.Binary.Put (runPut, Put, putByteString)

import Data.Enumerator (yield, continue, Iteratee, Stream(..))
import qualified Data.Attoparsec as Atto
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy.Char8 as B

import Database.Altis.Storage
import Database.Altis.Commands

requestHandler :: AltisDataset -> Socket -> Iteratee AltisRequest IO ()
requestHandler db s = do
    continue requestConsume
  where
    requestConsume :: Stream AltisRequest -> Iteratee AltisRequest IO ()
    requestConsume (Chunks mrs) = do
        liftIO $ mapM_ (process) mrs
        continue requestConsume
    requestConsume EOF = do
        yield () EOF
    process = processRequest db s

processRequest :: AltisDataset -> Socket -> AltisRequest -> IO ()
processRequest db s req = do
    response <- command db req
    let out = runPut $ putRedisResponse response
    BinSock.sendAll s out

upperBs :: S.ByteString -> S.ByteString
upperBs s = S.map wordUpper s
  where
    wordUpper c | c >='a' && c <='z' = chr (ord c - 32)
                | otherwise          = c

parseRequest :: Atto.Parser AltisRequest
parseRequest = do
    void $ Atto.string "*"
    numArgs <- readIntLine
    allargs <- replicateM numArgs readArg

    let (cmd:rest) = allargs

    return $ AltisRequest (upperBs cmd) rest

readArg :: Atto.Parser S.ByteString
readArg = do
    void $ Atto.string "$"
    size <- readIntLine

    val <- Atto.take size
    void $ Atto.string "\r\n"
    return val

readIntLine :: Atto.Parser Int
readIntLine = do
    line <- readLineContents
    return $ read $ S.unpack line

readLineContents :: Atto.Parser S.ByteString
readLineContents = do
    v <- Atto.takeTill (==13)
    _ <- Atto.string "\r\n"
    return v

serverListenLoop :: AltisDataset -> Socket -> IO ()
serverListenLoop db s = do
    listen s 100
    forever $ do
        (c, _) <- accept s
        h <- socketToHandle c ReadMode
        hSetBuffering h NoBuffering
        forkIO $ connHandler c h db

connHandler :: Socket -> Handle -> AltisDataset -> IO ()
connHandler c h db = (do
    let enum = enumHandle 32768 h
    let parser = iterParser parseRequest
    void $ run_ (enum $$ E.sequence parser $$ requestHandler db c)
    ) `finally` hClose h

-- implied, port number etc
runAltisServer :: AltisDataset -> IO ()
runAltisServer db = do
    proto <- getProtocolNumber "tcp"
    s <- socket AF_INET Stream proto
    setSocketOption s ReuseAddr 1
    bindSocket s (SockAddrInet 6001 iNADDR_ANY)
    void $ forkIO $ serverListenLoop db s

putRedisResponse :: AltisResponse -> Put 
putRedisResponse (AltisBulkResponse NullResponse) = putByteString "$-1\r\n"
putRedisResponse (AltisBulkResponse (StringResponse s)) = do
    putByteString "$"
    putByteString (S.pack $ show $ S.length s)
    putByteString "\r\n"
    putByteString s
    putByteString "\r\n"

putRedisResponse (AltisLineResponse s) = do
    putByteString "+"
    putByteString s
    putByteString "\r\n"

putRedisResponse (AltisErrorResponse s) = do
    putByteString "-"
    putByteString s
    putByteString "\r\n"

putRedisResponse (AltisIntResponse i) = do
    putByteString ":"
    putByteString $ S.pack $ show i
    putByteString "\r\n"

putRedisResponse (AltisMultiBulkResponse xs) = do
    putByteString "*"
    putByteString (S.pack $ show $ length xs)
    putByteString "\r\n"
    mapM_ (putRedisResponse . AltisBulkResponse) xs
