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

  , write
  ) where

import Control.Exception
import Control.Monad.ST (runST)
import Data.Bifunctor (first)
import Data.Primitive.ByteArray
import Data.Primitive.ByteArray.Unaligned
import Data.Primitive.ByteArray.Offset
import Data.Primitive.Types
import Data.Primitive.Unlifted.Array
import Data.Word
import Data.Vector (Vector)
import Foreign.C.Types
import GHC.Exts
import Net.IPv4 (IPv4(..))
import Socket.Stream.IPv4 hiding (connect)

import qualified Data.Vector as V
import qualified Socket.Stream.IPv4 as S
import qualified Builder as B
import qualified Data.ByteArray.Builder as BB

--newtype Carbon = Carbon { getCarbon :: Connection }

-- | A connection to a carbon database.
data Carbon = Carbon
  { metadata :: !(MutableByteArray RealWorld)
    -- Inside this mutable byte array, we have:
    -- 1. Reconnection count: Word64
    -- 2. Active connection: Connection (CInt), given 64 bits of space
    -- 3. Peer IPv4 (static): Word32
    -- 4. Peer Port (static): Word16
    -- 5. Local Port: Word16
  , buffer :: !(MutableUnliftedArray RealWorld (MutableByteArray RealWorld))
  }

data CarbonException
  = CarbonConnectException (ConnectException ('Internet 'V4) 'Uninterruptible)
  | CarbonSendException (SendException 'Uninterruptible)
  | CarbonReceiveException (ReceiveException 'Uninterruptible)
  | CarbonResponseException
  | CarbonCloseException CloseException

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
  (  B.buildByteArray path 0 (sizeofByteArray path)
  <> B.buildWord8 space
  <> B.buildByteArray value 0 (sizeofByteArray value)
  <> B.buildWord8 space
  <> B.buildWord64 timestamp
  <> B.buildWord8 newline
  )

write :: Carbon -> Vector Point -> IO (Either CarbonException ())
write c@Carbon{buffer} = \ !points0 ->
  getConnection c >>= \case
    Left err -> pure (Left err)
    Right conn -> do
      flip foldMap points0 $ \(encodePoint -> point) ->
        send conn (MutableBytes

create :: Peer -> IO (Either CarbonException Carbon)
create p@Peer{address,port} = do
  -- TODO: mask exceptions
  S.connect p >>= \case
    Left err -> pure (Left (CarbonConnectException err))
    Right connection@(Connection c) -> do
      metadata <- do
        marr <- newByteArray metadataSize
        writeUnalignedByteArray marr 0 (0 :: Word64)
        writeUnalignedByteArray marr 8 c
        writeUnalignedByteArray marr 16 (getIPv4 address)
        writeUnalignedByteArray marr 20 port
        writeUnalignedByteArray marr 22 (1 :: Word16)
        pure marr
      buffer <- do
        marr <- unsafeNewUnliftedArray bufferSize
        writeUnliftedArray marr 0 =<< newByteArray minimumBufferSize
        pure marr
      pure (Right Carbon{..})

minimumBufferSize :: Int
minimumBufferSize = 8192 - (2 * sizeOf (undefined :: Int))

bufferSize :: Int
bufferSize = 1

metadataSize :: Int
metadataSize = 24

incrementConnectionCount :: Carbon -> IO ()
incrementConnectionCount Carbon{metadata} = do
  n :: Word64 <- readUnalignedByteArray metadata 0
  writeUnalignedByteArray metadata 0 (n + 1)

writeActiveConnection :: Carbon -> Connection -> IO ()
writeActiveConnection Carbon{metadata} (Connection c) =
  writeUnalignedByteArray metadata 8 c

writeLocalPort :: Carbon -> Word16 -> IO ()
writeLocalPort Carbon{metadata} port =
  writeUnalignedByteArray metadata 22 port

readPeerIPv4 :: Carbon -> IO IPv4
readPeerIPv4 Carbon{metadata} = do
  w <- readUnalignedByteArray metadata 16
  pure (IPv4 w)

readPeerPort :: Carbon -> IO Word16
readPeerPort Carbon{metadata} = do
  readUnalignedByteArray metadata 20

readActiveConnection :: Carbon -> IO (Maybe Connection)
readActiveConnection Carbon{metadata} = do
  w :: Word16 <- readUnalignedByteArray metadata 22
  case w of
    0 -> pure Nothing
    _ -> do
      c <- readUnalignedByteArray metadata 8
      pure (Just (Connection c))

connected :: Carbon -> IO (Either CarbonException ())
connected inf = readActiveConnection inf >>= \case
  Nothing -> connect inf >>= \case
    Left err -> pure (Left err)
    Right _ -> pure (Right ())
  Just _ -> pure (Right ())

disconnected :: Carbon -> IO (Either CarbonException ())
disconnected inf = readActiveConnection inf >>= \case
  Nothing -> pure (Right ())
  Just conn -> do
    writeLocalPort inf 0
    fmap (first CarbonCloseException) (S.disconnect conn)

connect :: Carbon -> IO (Either CarbonException Connection)
connect inf = do
  address <- readPeerIPv4 inf
  port <- readPeerPort inf
  -- TODO: Mask exceptions
  S.connect Peer{address,port} >>= \case
    Left err -> pure (Left (CarbonConnectException err))
    Right conn -> do
      writeActiveConnection inf conn
      incrementConnectionCount inf
      pure (Right conn)

getConnection :: Carbon -> IO (Either CarbonException Connection)
getConnection inf = readActiveConnection inf >>= \case
  Nothing -> connect inf
  Just conn -> pure (Right conn)

with :: ()
  => Peer
  -> (Carbon -> IO a)
  -> IO (Either CarbonException (), Maybe a)
with p f = create p >>= \case
  Left err -> pure (Left err, Nothing)
  Right c -> do
    mask $ \restore -> do
      a <- onException (restore (f c)) (disconnected c)
      e <- disconnected c
      pure (e, Just a)
