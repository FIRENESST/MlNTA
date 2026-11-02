{-# LANGUAGE OverloadedStrings #-}
-- | Haskell client for BEEP backends via the beepgw HTTP gateway (http-client, http-types).
-- Test: Test.hs.
--
-- > mgr <- newManager defaultManagerSettings
-- > r <- beepCall mgr "POST" "/orders" [("Content-Type", "application/json")] "{\"id\":7}"
module Beep (BeepError (..), BeepResponse (..), beepCall) where

import Control.Exception (Exception, throwIO)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Network.HTTP.Client
import Network.HTTP.Types (Header, statusCode)
import System.Environment (lookupEnv)

data BeepError = BeepError {beepCode :: Int, httpStatus :: Int, beepBody :: BL.ByteString}
  deriving (Show)

instance Exception BeepError

data BeepResponse = BeepResponse
  { status :: Int,
    headers :: [Header],
    body :: BL.ByteString
  }
  deriving (Show)

beepCall :: Manager -> BS.ByteString -> String -> [Header] -> BL.ByteString -> IO BeepResponse
beepCall mgr method target hdrs reqBody = do
  gw <- maybe "http://127.0.0.1:8080" id <$> lookupEnv "BEEP_GATEWAY"
  initial <- parseRequest (gw ++ target)
  let req = initial {method = method, requestHeaders = hdrs, requestBody = RequestBodyLBS reqBody}
  res <- httpLbs req mgr
  let st = statusCode (responseStatus res)
  case lookup "x-beep-error-code" (responseHeaders res) of
    Just code -> throwIO (BeepError (read (BC.unpack code)) st (responseBody res))
    Nothing -> pure (BeepResponse st (responseHeaders res) (responseBody res))
