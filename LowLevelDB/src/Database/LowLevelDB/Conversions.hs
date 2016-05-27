-- | Conversions methods
module Database.LowLevelDB.Conversions
 ( toWord8s
 , fromWord8s
 , toWord4s
 , fromWord4s
 , toBits
 , fromBits
 )where

import Data.List
import Data.Word
import Data.Bits

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
