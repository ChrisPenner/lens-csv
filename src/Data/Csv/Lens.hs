{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
module Data.Csv.Lens where

import Control.Lens
import qualified Data.ByteString.Lazy as BL
import Data.Csv
import qualified Data.Csv.Streaming as S
import Data.Foldable
import qualified Data.Vector as V
import Data.Either
import System.IO.Unsafe

data IndexedRecord i where
  RecordWithName :: Header -> NamedRecord -> IndexedRecord Name
  RecordWithIndex :: Record -> IndexedRecord Int

data Csv' n where
  NamedCsv :: Header -> S.Records NamedRecord -> Csv' Name
  UnnamedCsv :: S.Records Record -> Csv' Int

namedCsv :: Prism' BL.ByteString (Csv' Name)
namedCsv = prism' embed project
  where
    embed :: Csv' Name -> BL.ByteString
    embed (NamedCsv headers xs) = encodeByName headers (toList xs)
    project :: BL.ByteString -> Maybe (Csv' Name)
    project = fmap (uncurry NamedCsv) . preview _Right . S.decodeByName

csv :: Iso' BL.ByteString (Csv' Int)
csv = iso project embed
  where
    embed :: Csv' Int -> BL.ByteString
    embed (UnnamedCsv xs) = encode (toList xs)
    project :: BL.ByteString -> (Csv' Int)
    project = UnnamedCsv . S.decode NoHeader

asList :: Iso (V.Vector a) (V.Vector b) [a] [b]
asList = iso V.toList V.fromList

unpackRecordWithName :: IndexedRecord Name -> NamedRecord
unpackRecordWithName (RecordWithName _ r) = r

unpackRecordWithIndex :: IndexedRecord Int -> Record
unpackRecordWithIndex (RecordWithIndex r) = r

headers :: IndexedTraversal' Int (Csv' Name) Name
headers  f (NamedCsv h xs) = flip NamedCsv xs <$> (h & traversed %%@~ indexed f)

rows :: IndexedTraversal' Int (Csv' i) (IndexedRecord i)
rows f (NamedCsv h xs) = NamedCsv h . fmap unpackRecordWithName <$> (xs & traversed %%@~ \i x -> indexed f i (RecordWithName h x))
rows f (UnnamedCsv xs) = UnnamedCsv . fmap unpackRecordWithIndex <$> (xs & traversed %%@~ \i x -> indexed f i (RecordWithIndex x))

namedRec :: IndexPreservingTraversal' (IndexedRecord Name) NamedRecord
namedRec = cloneIndexPreservingTraversal t
  where
    t :: Traversal' (IndexedRecord Name) NamedRecord
    t f (RecordWithName h r) = RecordWithName h <$> f r

namedRec' :: (FromNamedRecord a, ToNamedRecord b) => IndexPreservingTraversal (IndexedRecord Name) (IndexedRecord Name) a b
namedRec' = namedRec . _NamedRecord'

rec :: IndexPreservingTraversal' (IndexedRecord Int) Record
rec = cloneIndexPreservingTraversal t
  where
    t :: Traversal' (IndexedRecord Int) Record
    t f (RecordWithIndex r) = RecordWithIndex <$> f r

columns :: IndexedTraversal' i (IndexedRecord i) Field
columns f (RecordWithIndex r) = RecordWithIndex <$> (r & itraversed %%@~ indexed f)
columns f (RecordWithName h r) = RecordWithName h <$> (r & itraversed %%@~ indexed f)

_Record :: (FromRecord a, ToRecord a) => Prism' Record a
_Record = _Record'

_Record' :: (FromRecord a, ToRecord b) => Prism Record Record a b
_Record' = prism embed project
  where
    project s = either (const $ Left s) Right . runParser . parseRecord $ s
    embed = toRecord

_NamedRecord :: (FromNamedRecord a, ToNamedRecord a) => Prism' NamedRecord a
_NamedRecord = _NamedRecord'

_NamedRecord' :: (FromNamedRecord a, ToNamedRecord b) => Prism NamedRecord NamedRecord a b
_NamedRecord' = prism embed project
  where
    project s = either (const $ Left s) Right . runParser . parseNamedRecord $ s
    embed = toNamedRecord

_Field :: (FromField a, ToField a) => Prism' Field a
_Field = _Field'

_Field' :: (FromField a, ToField b) => Prism Field Field a b
_Field' = prism embed project
  where
    project s = either (const $ Left s) Right . runParser . parseField $ s
    embed = toField

testFile :: BL.ByteString
testFile = unsafePerformIO $ BL.readFile "./data/florida.csv"
