{-# LANGUAGE OverloadedStrings #-}
-- runghc Test.hs   (BEEP_GATEWAY env var; needs bytestring, http-client and http-types)
import Beep
import Control.Exception (try)
import Control.Monad (unless)
import Data.IORef
import Network.HTTP.Client (defaultManagerSettings, newManager)
import System.Exit

main :: IO ()
main = do
  mgr <- newManager defaultManagerSettings
  fails <- newIORef (0 :: Int)
  let check ok msg = unless ok $ modifyIORef fails (+ 1) >> putStrLn ("FAIL: " ++ msg)
  r <- beepCall mgr "POST" "/echo" [("Content-Type", "text/plain"), ("X-Test", "haskell")] "hello from haskell"
  check (status r == 200 && body r == "hello from haskell" && lookup "x-test" (headers r) == Just "haskell") ("echo " ++ show r)
  r2 <- beepCall mgr "GET" "/stream/3" [] ""
  check (body r2 == "line 0\nline 1\nline 2\n") "stream"
  e <- try (beepCall mgr "GET" "/err/537" [] "")
  case e of
    Left (BeepError code st _) -> check (code == 537 && st == 403) "err code"
    Right _ -> check False "expected BeepError"
  r4 <- beepCall mgr "GET" "/status/404" [] ""
  check (status r4 == 404) "status"
  n <- readIORef fails
  if n == 0
    then putStrLn "Haskell (gateway): all tests passed" >> exitSuccess
    else putStrLn ("Haskell (gateway): " ++ show n ++ " FAILURE(S)") >> exitFailure
