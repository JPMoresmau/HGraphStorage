{-# LANGUAGE ScopedTypeVariables #-}
-- | Conversions methods, between user types and a format suitable for Tries and TxTries
module Database.LowLevelDB.Conversions
 ( toWord8s
 , fromWord8s
 , toWord4s
 , fromWord4s
 , toBits
 , fromBits
 , TxKey(..)
 )where

import Data.List
import Data.Word
import Data.Bits
import Data.Int

-- | Class for keys usable in a TxTrie (in conjunction with a transaction id)
class TxKey a where
    -- | get the prefix for the key without the transaction id
    keyPrefix :: a -> [Word8]
    -- | get the full key data for key and transaction id
    toKey :: (a,Word64) -> [Word8]
    -- | get the key and transaction id from the data
    fromKey :: [Word8] -> (a,Word64)

-- | Word64 TxKey instance
instance TxKey Word64 where
    keyPrefix = intPrefix
    toKey = intToKey
    fromKey  = foldr unstep (0,0)
       where
          unstep b (a,c)
            | b==intSeparator = (c,a)
            | otherwise = (a`shiftL` 4 .|. fromIntegral b,c)

-- | Word32 TxKey instance
instance TxKey Word32 where
    keyPrefix = intPrefix
    toKey = intToKey
    fromKey = intFromKey

-- | Int64 TxKey instance
instance TxKey Int64 where
    keyPrefix = intPrefix
    toKey = intToKey
    fromKey = intFromKey

-- | Int32 TxKey instance
instance TxKey Int32 where
    keyPrefix = intPrefix
    toKey = intToKey
    fromKey = intFromKey

-- | Int TxKey instance
instance TxKey Int where
    keyPrefix = intPrefix
    toKey = intToKey
    fromKey = intFromKey

-- | Separator between key and transaction id
-- since we map on 4 bytes, 16 is outside the range and is a recognizable separator
intSeparator :: Word8
intSeparator = 16

-- | Key Prefix implementation for integrals
intPrefix :: (Integral a, Bits a) =>  a -> [Word8]
intPrefix a= toWord4s a ++ [intSeparator]

-- | Key Data implementation for integrals
intToKey :: (Integral a, Bits a) => (a,Word64) -> [Word8]
intToKey (a,k) = intPrefix a ++ toWord4s k

-- | Key data to Key and Transaction ID for integrals
intFromKey :: (Integral a, Bits a) =>  [Word8] -> (a,Word64)
intFromKey  ls =
        let (a,b,_) = foldr unstep (0,0,False) ls
        in (a,b)
       where
          unstep b (a,c,fl)
            | b == intSeparator = (a,c,True)
            | fl = (a `shiftL` 4 .|. fromIntegral b,c,fl)
            | otherwise= (a,c`shiftL` 4 .|. fromIntegral b,fl)

-- | Convert an integral into an array of Word8
toWord8s :: (Integral a, Bits a) =>  a -> [Word8]
toWord8s = to 255 8

-- | Convert an integral into an array of Word8, each Word8 representing only a bit (1 or 0)
toBits :: (Integral a, Bits a) =>  a -> [Word8]
toBits = to 1 1

-- | Convert an integral into an array of Word8, each Word8 representing only half a byte (0 to 15)
toWord4s :: (Integral a, Bits a) =>  a -> [Word8]
toWord4s = to 15 4

-- | Converts by masking and shifting
to ::  (Integral a, Bits a) =>  a -> Int -> a -> [Word8]
to _ _ a | a<0 = error "cannot convert negative numbers"
to mask shiftV a = unfoldr s a
    where
        s 0 = Nothing
        s i  = Just (fromIntegral (i .&. mask), shiftR i shiftV)

-- | Convert an array of Word8 into an integral
fromWord8s :: (Num a,Bits a)=> [Word8] -> a
fromWord8s = from 8

-- | Convert an array of Word8 representing half a byte into an integral
fromWord4s :: (Num a,Bits a)=> [Word8] -> a
fromWord4s  = from 4


-- | Convert an array of Word8 representing a bit into an integral
fromBits :: (Num a,Bits a)=> [Word8] -> a
fromBits  = from 1

--  | converts by shifting accumulator
from :: (Num a,Bits a)=> Int -> [Word8] -> a
from shiftV = foldr unstep 0
  where
    unstep b a = a `shiftL` shiftV .|. fromIntegral b
