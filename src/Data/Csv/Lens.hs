{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
module Data.Csv.Lens where

import Control.Lens
import qualified Data.ByteString.Lazy as BL
import Data.Csv hiding (index)
import qualified Data.Csv.Streaming as S
import Data.Foldable
import qualified Data.Vector as V
import Data.Either
import System.IO.Unsafe

data IndexedRecord i where
  RecordWithName :: Header -> NamedRecord -> IndexedRecord Name
  RecordWithIndex :: Record -> IndexedRecord Int

type instance Index (IndexedRecord i) = i
type instance IxValue (IndexedRecord i) = Field

instance Ixed (IndexedRecord i) where
  ix i f (RecordWithName h r) = RecordWithName h <$> (r & ix i %%~ f)
  ix i f (RecordWithIndex r) = RecordWithIndex <$> (r & ix i %%~ f)

data Csv' i where
  NamedCsv :: Header -> S.Records NamedRecord -> Csv' Name
  UnnamedCsv :: S.Records Record -> Csv' Int

type instance Index (Csv' i) = Int
type instance IxValue (Csv' i) = IndexedRecord i

instance Ixed (Csv' i) where
  ix i = rows . index i

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

columns :: IndexedTraversal' i (IndexedRecord i) Field
columns f (RecordWithIndex r) = RecordWithIndex <$> (r & itraversed %%@~ indexed f)
columns f (RecordWithName h r) = RecordWithName h <$> (r & itraversed %%@~ indexed f)

column :: Eq i => i -> IndexedTraversal' i (IndexedRecord i) Field
column i f x = x & ix i %%~ indexed f i

-- row :: Int -> IndexedTraversal' Int ( i) Field
-- row i f x = x & ix i %%~ indexed f i


_Record :: (FromRecord a, ToRecord a) => Prism' (IndexedRecord Int) a
_Record = _Record'

_Record' :: forall a b. (FromRecord a, ToRecord b) => Prism (IndexedRecord Int) (IndexedRecord Int) a b
_Record' = prism embed project
  where
    project :: IndexedRecord Int -> Either (IndexedRecord Int) a
    project (RecordWithIndex r) =
      case runParser (parseRecord r) of
        Left _ -> Left (RecordWithIndex r)
        Right a -> Right a
    embed :: b -> IndexedRecord Int
    embed = RecordWithIndex . toRecord

_NamedRecord :: (FromNamedRecord a, ToNamedRecord a) => IndexPreservingTraversal' (IndexedRecord Name) a
_NamedRecord = cloneIndexPreservingTraversal _NamedRecord'

_NamedRecord' :: forall a b. (FromNamedRecord a, ToNamedRecord b) => IndexPreservingTraversal (IndexedRecord Name) (IndexedRecord Name) a b
_NamedRecord' = cloneIndexPreservingTraversal t
  where
    t :: Traversal (IndexedRecord Name) (IndexedRecord Name) a b
    t f (RecordWithName h r) =
      case runParser (parseNamedRecord r) of
        Left _ -> pure (RecordWithName h r)
        Right a -> RecordWithName h . toNamedRecord <$> f a

_Field :: (FromField a, ToField a) => Prism' Field a
_Field = _Field'

_Field' :: (FromField a, ToField b) => Prism Field Field a b
_Field' = prism embed project
  where
    project s = either (const $ Left s) Right . runParser . parseField $ s
    embed = toField

testFile :: BL.ByteString
testFile = unsafePerformIO $ BL.readFile "./data/florida.csv"
