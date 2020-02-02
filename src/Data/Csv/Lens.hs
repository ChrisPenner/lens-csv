{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Data.Csv.Lens
    ( CsvRecord
    , Csv'
    , namedCsv
    , csv
    , headers
    , rows
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
    ) where

import Control.Lens
import qualified Data.ByteString.Lazy as BL
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

-- | A prism which attempts to parse a 'BL.ByteString' into a structured @'Csv'' 'Name'@.
namedCsv :: Prism' BL.ByteString (Csv' Name)
namedCsv = prism' embed project
  where
    embed :: Csv' Name -> BL.ByteString
    embed (NamedCsv headers xs) = encodeByName headers (toList xs)
    project :: BL.ByteString -> Maybe (Csv' Name)
    project = fmap (uncurry NamedCsv) . preview _Right . S.decodeByName

-- | A prism which attempts to parse a 'BL.ByteString' into a structured @'Csv'' 'Int'@.
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

-- | An indexed traversal over the CSV headers of a named CSV. Indexed by the column number
-- starting at 0.
headers :: IndexedTraversal' Int (Csv' Name) Name
headers  f (NamedCsv h xs) = flip NamedCsv xs <$> (h & traversed %%@~ indexed f)

-- | An indexed traversal over each row of the csv as a 'CsvRecord'. Passes through
-- a type witness signifying whether the records are 'Name' or 'Int' indexed.
rows :: IndexedTraversal' Int (Csv' i) (CsvRecord i)
rows f (NamedCsv h xs) = NamedCsv h . fmap unpackRecordWithName <$> (xs & traversed %%@~ \i x -> indexed f i (NamedCsvRecord x))
rows f (UnnamedCsv xs) = UnnamedCsv . fmap unpackRecordWithIndex <$> (xs & traversed %%@~ \i x -> indexed f i (CsvRecord x))

-- | Traverse the columns of a 'CsvRecord', indexed by either the column headers or column
-- indexes accordingly.
columns :: forall a i. (ToField a, FromField a) => IndexedTraversal' i (CsvRecord i) a
columns = columns'

-- | A more flexible version of 'columns' which allows the focused field to change types. Affords worse type inference, so prefer 'columns' when possible.
columns' :: forall a b i. (FromField a, ToField b) => IndexedTraversal i (CsvRecord i) (CsvRecord i) a b
columns' = cols . _Field'
  where
    cols :: IndexedTraversal' i (CsvRecord i) Field
    cols f (CsvRecord r) = CsvRecord <$> (r & itraversed %%@~ indexed f)
    cols f (NamedCsvRecord r) = NamedCsvRecord <$> (r & itraversed %%@~ indexed f)

-- | Select a specific column of a record by the appropriate index type, either 'Name' or 'Int' accordingly.
column :: forall a b i. (Eq i, FromField a, ToField a) => i -> IndexedTraversal' i (CsvRecord i) a
column i = column' i

-- | A more flexible version of 'column' which allows the focused field to change types. Affords worse type inference, so prefer 'column' when possible.
column' :: forall a b i. (Eq i, FromField a, ToField b) => i -> IndexedTraversal i (CsvRecord i) (CsvRecord i) a b
column' i =  t . _Field'
  where
    t :: IndexedTraversal' i (CsvRecord i) Field
    t f x = x & ix i %%~ indexed f i

-- | Traverse a specific row of the csv by row number.
row :: Int -> IndexedTraversal' Int (Csv' i) (CsvRecord i)
row i f x = x & ix i %%~ indexed f i

-- | Attempt to parse the given record into a type using 'FromRecord'.
_Record :: forall a b. (FromRecord a, ToRecord a) => Prism' (CsvRecord Int) a
_Record = _Record'

-- | A more flexible version of '_Record' which allows the focus to change types. Affords worse type inference, so prefer '_Record' when possible.
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
_NamedRecord :: forall a b. (FromNamedRecord a, ToNamedRecord a)
             => Prism' (CsvRecord Name) a
_NamedRecord = _NamedRecord'

-- | A more flexible version of '_NamedRecord' which allows the focus to change types. Affords worse type inference, so prefer '_NamedRecord' when possible.
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
_Field :: forall a. (FromField a, ToField a) => Prism' Field a
_Field = _Field'

-- | A more flexible version of '_Field' which allows the focus to change types. Affords worse type inference, so prefer '_Field' when possible.
_Field' :: forall a b. (FromField a, ToField b) => Prism Field Field a b
_Field' = prism embed project
  where
    project s = either (const $ Left s) Right . runParser . parseField $ s
    embed = toField
