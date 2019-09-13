{-# language
    BangPatterns
  , DataKinds
  , LambdaCase
  , NamedFieldPuns
  , RecordWildCards
  , ScopedTypeVariables
  , TypeApplications
  , ViewPatterns
  #-}

module Carbon
  ( Carbon(..)
  , CarbonException(..)
  , Point(..)

  , write
  , with
  ) where

import Control.Monad.ST (runST)
import Data.Primitive.Array
import Data.Primitive.ByteArray
import Data.Primitive.Unlifted.Array
import Data.Word
import Socket.Stream.IPv4 hiding (connect)
import Socket.Stream.Uninterruptible.Bytes (sendMany)

import qualified Builder as B

newtype Carbon = Carbon { getCarbon :: Connection }

data CarbonException
  = CarbonConnectException
      (ConnectException ('Internet 'V4) 'Uninterruptible)
  | CarbonSendException
      (SendException 'Uninterruptible)
  | CarbonCloseException
      CloseException

-- <metric path> <metric value> <metric timestamp>
data Point = Point
  { path  :: !ByteArray
  , value :: !ByteArray
  , timestamp :: !Word64
  }

space, newline :: Word8
space = 32
newline = 10

encodePoint :: Point -> ByteArray
encodePoint Point{path,value,timestamp} = B.build
  (  B.bytearray path 0 (sizeofByteArray path)
  <> B.word8 space
  <> B.bytearray value 0 (sizeofByteArray value)
  <> B.word8 space
  <> B.word64 timestamp
  <> B.word8 newline
  )

encodePoints :: Array Point -> UnliftedArray ByteArray
encodePoints = \ !arr -> runST $ do
  let !sz = length arr
  !marr <- unsafeNewUnliftedArray sz
  let go !ix = if ix < sz
        then do
          x <- indexArrayM arr ix
          writeUnliftedArray marr ix (encodePoint x)
          go (ix + 1)
        else pure ()
  go 0
  unsafeFreezeUnliftedArray marr

write :: Carbon -> Array Point -> IO (Either CarbonException ())
write (Carbon c) = \ !points -> do
  r <- sendMany c (encodePoints points)
  case r of
    Left err -> pure (Left (CarbonSendException err))
    Right () -> pure (Right ())

with :: ()
  => Peer
  -> (Carbon -> IO a)
  -> IO (Either CarbonException a)
with peer f = do
  r <- withConnection
    peer
    (\e a -> case e of
      Left c -> pure (Left (CarbonCloseException c))
      Right () -> pure a
    )
    (\(Carbon -> conn) -> fmap Right (f conn)
    )
  case r of
    Left e -> pure (Left (CarbonConnectException e))
    Right x -> pure x
