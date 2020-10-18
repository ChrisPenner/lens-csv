{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module Data.Csv.LensSpec where

import Data.ByteString.Lazy as BL
import Test.Tasty
import Test.Tasty.Hspec

import Control.Lens (taking)
import Control.Lens.Operators
import Data.Csv.Lens
import Data.Map (Map)
import qualified Data.Map as Map

spec_lens_csv :: Spec
spec_lens_csv = describe "transforming simple.csv" $ do

  it "should extract state codes" $ do
    myCsv <- BL.readFile "./data/simple.csv"
    let result = myCsv ^.. namedCsv . taking 2 rows . column @String "state_code"
    result `shouldBe` [ "NY", "CA" ]

  it "should be able to extract records as maps" $ do
    myCsv <- BL.readFile "./data/simple.csv"
    let result = myCsv ^.. namedCsv . taking 2 rows . _NamedRecord @(Map String String)
    result `shouldBe` [ Map.fromList [("population","19540000"),("state_code","NY")]
                      , Map.fromList [("population","39560000"),("state_code","CA")]
                      ]

  it "should allow column indexing using Int" $ do
    myCsv <- BL.readFile "./data/simple.csv"
    let result = myCsv ^.. csv . rows . column @Int 1
    result `shouldBe` [ 19540000, 39560000 ]

  it "should be possible to edit using traversals" $ do
    myCsv <- BL.readFile "./data/simple.csv"
    let result = myCsv & namedCsv . row 1 . column @Int "population" +~ 1337
    result `shouldBe` "state_code,population\r\nNY,19540000\r\nCA,39561337\r\n"
