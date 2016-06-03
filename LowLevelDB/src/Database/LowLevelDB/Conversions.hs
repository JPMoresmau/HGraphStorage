{-# LANGUAGE ScopedTypeVariables #-}
-- | Conversions methods
module Database.LowLevelDB.Conversions
 ( toWord8s
 , fromWord8s
 , toWord4s
 , fromWord4s
 , tupleToWord4s
 , tupleFromWord4s
 , tripleToWord4s
 , tripleFromWord4s
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

tupleToWord4s :: (Integral a, FiniteBits a,Integral b,Bits b) => (a,b) -> [Word8]
tupleToWord4s (a,b) =
    let i::Integer = (fromIntegral b `shiftL` finiteBitSize a) + fromIntegral a
    in to 15 4 i

tripleToWord4s :: (Integral a, FiniteBits a,Integral b,FiniteBits b) => (a,b,b) -> [Word8]
tripleToWord4s (a,b,c) =
    let
        sh = finiteBitSize a
        i::Integer = (fromIntegral c `shiftL` (finiteBitSize b + sh)) + (fromIntegral b `shiftL` sh) + fromIntegral a
    in to 15 4 i

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

tupleFromWord4s :: forall a b.(Integral a,FiniteBits a,Integral b,Bits b)=> [Word8] ->  (a,b)
tupleFromWord4s  ls =
    let
        i::Integer = from 4 ls
        a =fromInteger (i `shiftR` l undefined)
        b = fromInteger (i .&. toInteger oneBits)
    in (b,a)
    where
        l :: a -> Int
        l = finiteBitSize
        oneBits :: a
        oneBits = complement zeroBits

tripleFromWord4s :: forall a b.(Integral a,FiniteBits a,Integral b,FiniteBits b)=> [Word8] ->  (a,b,b)
tripleFromWord4s  ls =
    let
        i::Integer = from 4 ls
        a =fromInteger ((i `shiftR` la undefined) .&. toInteger oneBitsb)
        c =fromInteger (i `shiftR` (la undefined+lb undefined))
        b = fromInteger (i .&. toInteger oneBitsa)
    in (b,a,c)
    where
        la :: a -> Int
        la = finiteBitSize
        lb :: b -> Int
        lb = finiteBitSize
        oneBitsa :: a
        oneBitsa = complement zeroBits
        oneBitsb :: b
        oneBitsb = complement zeroBits

-- | Convert an array of Word8 representing a bit into an integral
fromBits :: (Num a,Bits a)=> [Word8] -> a
fromBits  = from 1

--  | converts by shifting accumulator
from :: (Num a,Bits a)=> Int -> [Word8] -> a
from shiftV = foldr unstep 0
  where
    unstep b a = a `shiftL` shiftV .|. fromIntegral b
