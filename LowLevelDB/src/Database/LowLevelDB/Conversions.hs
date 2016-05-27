-----------------------------------------------------------------------------
--
-- Module      :  Database.LowLevelDB.Conversions
-- Copyright   :
-- License     :  AllRightsReserved
--
-- Maintainer  :
-- Stability   :
-- Portability :
--
-- |
--
-----------------------------------------------------------------------------

module Database.LowLevelDB.Conversions where

import Data.List
import Data.Word
import Data.Bits


toWord8s :: (Integral a, Bits a) =>  a -> [Word8]
toWord8s = unfoldr s
    where
        s 0 = Nothing
        s i  = Just (fromIntegral i, shiftR i 8)

toBits :: (FiniteBits a) =>  a -> [Word8]
toBits b = map (\i->if testBit b i then 1 else 0) [0 .. finiteBitSize b - 1]


toWord4s :: (Integral a, Bits a) =>  a -> [Word8]
toWord4s = unfoldr s
    where
        s 0 = Nothing
        s i  = Just (fromIntegral (i .&. 15), shiftR i 4)

fromWord8s :: (Num a,Bits a)=> [Word8] -> a
fromWord8s  = foldr unstep 0
  where
    unstep b a = a `shiftL` 8 .|. fromIntegral b
