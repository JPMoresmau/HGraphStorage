# HGraphStorage

This project aims at implementing a simple graph database.
Compared to other Haskell projects, it doesn't work in memory but at the moment purely on disk.
The idea is to play around with storage techniques and hope that the OS disk cache will make performance acceptable.

## Storage
As much as possible, we try to rely on fixed length records so that an ID can easily translate into a file offset for quick reading.
The format of the files on disk is inspired by the Neo4J format as explained in various Neo4J presentations.

## Indices
There is also an on-disk implementation of a tree that can be used to index for example string values

## Transactions, concurrency
Nothing of the sort is implemented yet! Currently, the "database" consists of Handles to files, so no concurrent access is offered.
Again, this is more a research project to play around things that are not often discussed (database layout of files, etc.).

## Contributing
Feel free to contribute if you want! Currently there is a test suite and a benchmark alongside with the library code.
Looking at the tests and the benchmark will give you an idea of the API. It's very low level at the moment!
