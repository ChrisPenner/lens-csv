# lens-csv

A lensy layer on top of Cassava which affords streaming, traversable, CSV parsing.

Currently experimental (but working). Looking to improve error handling soon, currently parse failures are simply passed over by the traversals.

Example:

```haskell
>>> import Data.ByteString.Lazy as BL
>>> myCsvText <- BL.readFile "./data/simple.csv"
>>> myCsvText ^.. namedCsv . taking 2 rows . column @String "state_code" 
[ "NY"
, "CA"
]

>>> myCsvText ^.. namedCsv . taking 2 rows . _NamedRecord @[M.Map String String]
[ fromList [("population","19540000"), ("state_code","NY")]
, fromList [("population","39560000"), ("state_code","CA")]
]

-- For csv files without headers
>>> myCsvText ^.. csv . taking 2 rows . _Record @[String]
[ ["state_code", "population"]
, ["NY"        , "19540000"]
]

-- 'column' infers whether it's a named or unnamed csv and accepts the appropriate index type (either ByteString or Int)
>>> myCsvText ^.. csv . rows . column @Int 1
[19540000,39560000]

-- Use traversals to edit cells 'in-place' (add 1337 to California's population)
>>> BL.putStrLn $ myCsvText & namedCsv . row 1 . column @Int "population" +~ 1337
state_code,population
NY,19540000
CA,39561337
```
