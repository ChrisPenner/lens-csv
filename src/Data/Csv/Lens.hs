{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
module Data.Csv.Lens where

import Control.Lens
import qualified Data.ByteString.Lazy as BL
import Data.Csv
import Data.Foldable
import qualified Data.Vector as V
import Data.Either

data IndexedRecord i where
  RecordWithName :: Header -> NamedRecord -> IndexedRecord Name
  RecordWithIndex :: Record -> IndexedRecord Int

data Csv' n where
  NamedCsv :: Header -> V.Vector NamedRecord -> Csv' Name
  UnnamedCsv :: V.Vector Record -> Csv' Int


namedCsv :: Prism' BL.ByteString (Csv' Name)
namedCsv = prism' embed project
  where
    embed :: Csv' Name -> BL.ByteString
    embed (NamedCsv headers xs) = encodeByName headers (V.toList xs)
    project :: BL.ByteString -> Maybe (Csv' Name)
    project = fmap (uncurry NamedCsv) . preview _Right . decodeByName

csv :: Prism' BL.ByteString (Csv' Int)
csv = prism' embed project
  where
    embed :: Csv' Int -> BL.ByteString
    embed (UnnamedCsv xs) = encode (V.toList xs)
    project :: BL.ByteString -> Maybe (Csv' Int)
    project = fmap UnnamedCsv . preview _Right . decode NoHeader

asList :: Iso (V.Vector a) (V.Vector b) [a] [b]
asList = iso V.toList V.fromList

unpackRecordWithName :: IndexedRecord Name -> NamedRecord
unpackRecordWithName (RecordWithName _ r) = r

unpackRecordWithIndex :: IndexedRecord Int -> Record
unpackRecordWithIndex (RecordWithIndex r) = r


rows :: IndexedTraversal' Int (Csv' i) (IndexedRecord i)
rows f (NamedCsv h xs) = NamedCsv h . fmap unpackRecordWithName <$> (xs & rowsHelper %%@~ \i x -> indexed f i (RecordWithName h x))
rows f (UnnamedCsv xs) = UnnamedCsv . fmap unpackRecordWithIndex <$> (xs & rowsHelper %%@~ \i x -> indexed f i (RecordWithIndex x))

rowsHelper :: IndexedTraversal Int (V.Vector a) (V.Vector b) a b
rowsHelper = traversed

columns :: IndexedTraversal' i (IndexedRecord i) Field
columns f (RecordWithIndex r) = RecordWithIndex <$> (r & itraversed %%@~ indexed f)
columns f (RecordWithName h r) = RecordWithName h <$> (r & itraversed %%@~ indexed f)

_Field :: (FromField a, ToField a) => Prism' Field a
_Field = _Field'

_Field' :: (FromField a, ToField b) => Prism Field Field a b
_Field' = prism embed project
  where
    project s = either (const $ Left s) Right . runParser . parseField $ s
    embed = toField
