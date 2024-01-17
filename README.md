# proto-db

proto-db is a very basic implementation of a few major components of modern relational databases. It is not intended 
to be a fully functional database for use in production, but is instead an exercise examining the different aspects of 
such a DBMS and an implementation of a select number of these aspects. Below is a list of these components as well as 
some brief descriptions and notes.

Note: This repository was set up after the completion of the project and, as such, is missing a number of the original 
files, which may lead to unresolved references and other errors.

## index

This database implements an indexing system which uses a B+ tree data structure. This B+ tree is composed of 
inner nodes and leaf nodes. Inner nodes contain keys which facilitate the search aspect of the tree and leaf nodes 
contain the data which one aims to index (or at least a reference to the data in memory).

## query

We utilize a basic Selinger-style query optimization which includes pushing select/project operators below joins when
possible, considering only left-deep joins, and join operator optimization (based on minimizing IOs).

## concurrency 

Concurrency control is implemented in this database using a multigranular locking scheme. This locking scheme is 
facilitated by a Lock Manager which distributes requested locks as well as the implicitly necessary locks, and throws 
any necessary exceptions.

## recovery

This project uses an ARIES-style recovery procedure which allows flawless recovery from crash. This is accomplished 
mainly by using soft checkpointing, as well as redo/undo write ahead logging. In the ARIES style, we use a 
steal/no-force scheme for maximum efficiency.