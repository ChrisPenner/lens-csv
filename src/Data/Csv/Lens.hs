{-|
Module      : Data.Csv.Lens
Description : A lensy layer on top of Cassava which affords streaming, traversable, CSV parsing.
Copyright   : (c) Chris Penner, 2019
License     : BSD3

The examples below use the following csv as the value @myCsv@:

> state_code,population
> NY,19540000
> CA,39560000

-}

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Csv.Lens
    ( namedCsv
    , csv
    , headers
    , rows
    , row
    , columns
    , columns'
    , column
    , column'
    , _Record
    , _Record'
    , _NamedRecord
    , _NamedRecord'
    , _Field
    , _Field'
    , Csv'
    , CsvRecord
    , cassavaNamed
    , cassavaUnnamed
    ) where

import Control.Lens
import qualified Data.ByteString.Lazy as BL hiding (putStrLn)
import Data.Csv hiding (index)
import qualified Data.Csv.Streaming as S
import Data.Foldable
import Data.Either
import GHC.TypeLits
import Data.Kind
import Data.Type.Equality

-- $setup
-- >>> :set -XOverloadedStrings
-- >>> :set -XTypeApplications
-- >>> :set -XDataKinds
-- >>> import qualified Data.Map as M
-- >>> import qualified Data.ByteString.Lazy.Char8 as BL
-- >>> myCsv <- BL.readFile "./data/simple.csv"


-- | A CSV Record which carries a type-level witness of whether the record is named or not.
--
-- A csv record with named columns has type @'CsvRecord' 'Name'@ where 'Name' is simply an alias for 'BL.ByteString'
--
-- A csv record with numbered columns has type @'CsvRecord' 'Int'@
data CsvRecord i where
  NamedCsvRecord :: NamedRecord -> CsvRecord Name
  CsvRecord :: Record -> CsvRecord Int

instance Show (CsvRecord i) where
  showsPrec 11 r s = "(" <> showsPrec 0 r ")" <> s
  showsPrec _ (CsvRecord r) s = "CsvRecord (" <> show r <> ")" <> s
  showsPrec _ (NamedCsvRecord r) s = "NamedCsvRecord (" <> show r <> ")" <> s

type instance Index (CsvRecord i) = i
type instance IxValue (CsvRecord i) = Field

-- | 'CsvRecord's is indexable using 'ix' by either 'Int' for numbered columns or a 'Name' for
-- named columns.
instance Ixed (CsvRecord i) where
  ix i f (NamedCsvRecord r) = NamedCsvRecord <$> (r & ix i %%~ f)
  ix i f (CsvRecord r) = CsvRecord <$> (r & ix i %%~ f)

-- | 'Csv'' is a wrapper around cassava's csv type which carries the appropriate indexing
-- and column header information.
data Csv' i where
  NamedCsv :: Header -> S.Records NamedRecord -> Csv' Name
  UnnamedCsv :: S.Records Record -> Csv' Int

type instance Index (Csv' i) = Int
type instance IxValue (Csv' i) = CsvRecord i

-- | A 'Csv'' is indexable using 'ix' by either 'Int' or 'Name' respectively.
instance Ixed (Csv' i) where
  ix i = rows . index i

instance ToNamedRecord (CsvRecord Name) where
  toNamedRecord (NamedCsvRecord r) = r

instance ToRecord (CsvRecord Int) where
  toRecord (CsvRecord r) = r

instance FromNamedRecord (CsvRecord Name) where
  parseNamedRecord r = pure $ NamedCsvRecord r

instance FromRecord (CsvRecord Int) where
  parseRecord r = pure $ CsvRecord r


-- | An iso between the results of 'S.decodeByName' or 'S.decodeByNameWith' and a 'Csv'' for use with this library.
--
-- >>> S.decode HasHeader myCsv ^.. from cassavaUnnamed  . rows . column @String 0
-- ["NY","CA"]
cassavaUnnamed :: Iso' (Csv' Int) (S.Records Record)
cassavaUnnamed = iso (\(UnnamedCsv rs) -> rs) UnnamedCsv

-- | An iso between the results of 'S.decode' or 'S.decodeWith' and a 'Csv'' for use with this library.
--
-- You should typically just use 'namedCsv', but this can be helpful if you want to provide
-- special options to provide custom decoding options.
--
-- >>> S.decodeByName  myCsv ^.. _Right . from cassavaNamed . rows . column @String "state_code"
-- ["NY","CA"]
cassavaNamed :: Iso' (Csv' Name) (Header, S.Records NamedRecord)
cassavaNamed = iso (\(NamedCsv h rs) -> (h, rs)) (uncurry NamedCsv)

-- | A prism which attempts to parse a 'BL.ByteString' into a structured @'Csv'' 'Name'@.
--
-- This uses the first row of the csv as headers.
--
-- Note that this prism will silently fail to match if your CSV is malformed.
-- Follow up with 'rows', 'row', or 'headers'
--
-- >>> :t  myCsv ^? namedCsv
-- myCsv ^? namedCsv :: Maybe (Csv' Name)
namedCsv :: Prism' BL.ByteString (Csv' Name)
namedCsv = prism' embed project
  where
    embed :: Csv' Name -> BL.ByteString
    embed (NamedCsv headers xs) = encodeByName headers (toList xs)
    project :: BL.ByteString -> Maybe (Csv' Name)
    project = fmap (uncurry NamedCsv) . preview _Right . S.decodeByName

-- | A prism which attempts to parse a 'BL.ByteString' into a structured @'Csv'' 'Int'@.
--
-- Use this with CSVs which don't have a header row.
--
-- Note that this prism will silently fail to match if your CSV is malformed.
-- Follow up with 'rows' or 'row'
--
-- >>> :t  myCsv ^? csv
-- myCsv ^? csv :: Maybe (Csv' Int)
csv :: Iso' BL.ByteString (Csv' Int)
csv = iso project embed
  where
    embed :: Csv' Int -> BL.ByteString
    embed (UnnamedCsv xs) = encode (toList xs)
    project :: BL.ByteString -> (Csv' Int)
    project = UnnamedCsv . S.decode NoHeader

unpackRecordWithName :: CsvRecord Name -> NamedRecord
unpackRecordWithName (NamedCsvRecord r) = r

unpackRecordWithIndex :: CsvRecord Int -> Record
unpackRecordWithIndex (CsvRecord r) = r

-- | An indexed fold over the CSV headers of a named CSV. Indexed by the column number
-- starting at 0.
--
-- >>> myCsv ^.. namedCsv . headers
-- ["state_code","population"]
--
-- >>> myCsv ^@.. namedCsv . headers
-- [(0,"state_code"),(1,"population")]
headers :: IndexedFold Int (Csv' Name) Name
-- Note to self, this could technically be a traversal, but since we don't want to reparse all
-- records with the new headers we don't yet allow editing headers.
headers  f (NamedCsv h xs) = flip NamedCsv xs <$> (h & traversed %%@~ indexed f)

-- | An indexed traversal over each row of the csv as a 'CsvRecord'. Passes through
-- a type witness signifying whether the records are 'Name' or 'Int' indexed.
--
-- Traversing rows of a named csv results in named records:
--
-- >>> myCsv ^.. namedCsv . rows
-- [NamedCsvRecord (fromList [("population","19540000"),("state_code","NY")]),NamedCsvRecord (fromList [("population","39560000"),("state_code","CA")])]
--
-- Traversing rows of an indexed csv results in indexed records:
--
-- >>> myCsv ^.. csv . dropping 1 rows
-- [CsvRecord (["NY","19540000"]),CsvRecord (["CA","39560000"])]
rows :: IndexedTraversal' Int (Csv' i) (CsvRecord i)
rows f (NamedCsv h xs) = NamedCsv h . fmap unpackRecordWithName <$> (xs & traversed %%@~ \i x -> indexed f i (NamedCsvRecord x))
rows f (UnnamedCsv xs) = UnnamedCsv . fmap unpackRecordWithIndex <$> (xs & traversed %%@~ \i x -> indexed f i (CsvRecord x))

-- | Parse and traverse the fields of a 'CsvRecord' into the inferred 'FromField' type.
-- Focuses are indexed by either the column headers or column number accordingly.
--
-- Be careful to provide appropriate type hints to 'columns' so that it knows which 'Field'
-- type to parse into, any fields which fail to parse will be simply ignored, you can use this
-- strategically to select all fields of a given type within a record.
--
-- >>> myCsv ^.. namedCsv . row 0 . columns @String
-- ["19540000","NY"]
--
-- >>> myCsv ^.. namedCsv . row 0 . columns @Int
-- [19540000]
--
-- 'columns' is indexed, you can use the column number or column header.
--
-- >>> myCsv ^@.. namedCsv . row 0 . columns @String
-- [("population","19540000"),("state_code","NY")]
--
-- >>> myCsv ^@.. namedCsv . row 0 . columns @Int
-- [("population",19540000)]
--
--
-- >>> BL.lines (myCsv & namedCsv . rows . columns @Int %~ subtract 1)
-- ["state_code,population\r","NY,19539999\r","CA,39559999\r"]
columns :: forall a i. (ToField a, FromField a) => IndexedTraversal' i (CsvRecord i) a
columns = columns'

-- | A more flexible version of 'columns' which allows the focused field to change types. Affords worse type inference, so prefer 'columns' when possible.
--
-- See 'columns' for usage examples
columns' :: forall a b i. (FromField a, ToField b) => IndexedTraversal i (CsvRecord i) (CsvRecord i) a b
columns' = cols . _Field'
  where
    cols :: IndexedTraversal' i (CsvRecord i) Field
    cols f (CsvRecord r) = CsvRecord <$> (r & itraversed %%@~ indexed f)
    cols f (NamedCsvRecord r) = NamedCsvRecord <$> (r & itraversed %%@~ indexed f)

-- | Select a specific column of a record by the appropriate index type, either 'Name' for 'namedCsv's or 'Int' for 'csv's
--
-- See 'columns' for more usage ideas.
--
-- >>> myCsv ^.. namedCsv . rows . column @Int "population"
-- [19540000,39560000]
--
-- >>> myCsv ^.. csv . dropping 1 rows . column @String 0
-- ["NY","CA"]
column :: forall a b i. (Eq i, FromField a, ToField a) => i -> IndexedTraversal' i (CsvRecord i) a
column i = column' i

-- | A more flexible version of 'column' which allows the focused field to change types. Affords worse type inference, so prefer 'column' when possible.
--
-- See 'column' for usage examples
column' :: forall a b i. (Eq i, FromField a, ToField b) => i -> IndexedTraversal i (CsvRecord i) (CsvRecord i) a b
column' i =  t . _Field'
  where
    t :: IndexedTraversal' i (CsvRecord i) Field
    t f x = x & ix i %%~ indexed f i

-- | Traverse a specific row of the csv by row number.
row :: Int -> IndexedTraversal' Int (Csv' i) (CsvRecord i)
row i f x = x & ix i %%~ indexed f i

-- | A prism which attempt to parse the given record into a type using 'FromRecord'.
--
-- Tuples implement 'FromRecord':
--
-- >>> myCsv ^.. csv . row 1 . _Record @(String, Int)
-- [("NY",19540000)]
--
-- If we parse each row into a tuple record we can swap the positions and it will write back
-- into a valid CSV.
--
-- >>> import Data.Tuple (swap)
-- >>> BL.lines (myCsv & csv . rows . _Record @(String, String) %~ swap)
-- ["population,state_code\r","19540000,NY\r","39560000,CA\r"]
_Record :: forall a b. (FromRecord a, ToRecord a) => Prism' (CsvRecord Int) a
_Record = _Record'

-- | A more flexible version of '_Record' which allows the focus to change types. Affords worse type inference, so prefer '_Record' when possible.
--
-- See '_Record' for usage examples
_Record' :: forall a b. (FromRecord a, ToRecord b) => Prism (CsvRecord Int) (CsvRecord Int) a b
_Record' = prism embed project
  where
    project :: CsvRecord Int -> Either (CsvRecord Int) a
    project (CsvRecord r) =
      case runParser (parseRecord r) of
        Left _ -> Left (CsvRecord r)
        Right a -> Right a
    embed :: b -> CsvRecord Int
    embed = CsvRecord . toRecord

-- | Attempt to parse the given record into a type using 'FromNamedRecord'.
--
-- >>> myCsv ^? namedCsv . row 0 . _NamedRecord @(M.Map String String)
-- Just (fromList [("population","19540000"),("state_code","NY")])
_NamedRecord :: forall a b. (FromNamedRecord a, ToNamedRecord a)
             => Prism' (CsvRecord Name) a
_NamedRecord = _NamedRecord'

-- | A more flexible version of '_NamedRecord' which allows the focus to change types. Affords worse type inference, so prefer '_NamedRecord' when possible.
--
-- See '_NamedRecord' for usage examples
_NamedRecord' :: forall a b. (FromNamedRecord a, ToNamedRecord b)
              => Prism (CsvRecord Name) (CsvRecord Name) a b
_NamedRecord' = prism embed project
  where
    project :: CsvRecord Name -> Either (CsvRecord Name) a
    project (NamedCsvRecord r) =
      case runParser (parseNamedRecord r) of
        Left _ -> Left (NamedCsvRecord r)
        Right a -> Right a
    embed :: b -> CsvRecord Name
    embed = NamedCsvRecord . toNamedRecord

-- | Attempt to parse the given 'Field' into a type using 'FromField'.
--
-- You usually won't need this, 'column', 'columns', '_Record', and '_NamedRecord' are usually more flexible and provide more power.
_Field :: forall a. (FromField a, ToField a) => Prism' Field a
_Field = _Field'

-- | A more flexible version of '_Field' which allows the focus to change types. Affords worse type inference, so prefer '_Field' when possible.
--
-- You usually won't need this, 'column', 'columns', '_Record', and '_NamedRecord' are usually more flexible and provide more power.
_Field' :: forall a b. (FromField a, ToField b) => Prism Field Field a b
_Field' = prism embed project
  where
    project s = either (const $ Left s) Right . runParser . parseField $ s
    embed = toField
