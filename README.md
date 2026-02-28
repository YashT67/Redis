# Mini-Redis in C++

A lightweight, in-memory data store built from scratch in C++, supporting string key-value pairs and sorted sets. This project demonstrates core systems programming concepts, including custom data structures, event-driven networking, and memory management.

## 🏗️ Architecture & Key Features

* **Event-Driven Networking:** The server uses a single-threaded event loop with non-blocking I/O using `poll()` to efficiently handle multiple client connections concurrently.
* **Idle Connection Management:** Implemented an efficient timer system using an intrusive doubly linked list to track client activity. Idle connections are automatically evicted to prevent resource exhaustion, with timer updates operating in $O(1)$ time.
* **Custom Binary Protocol:** The client and server communicate via a custom networking protocol that serializes requests and responses using explicit data tags (Strings, Integers, Doubles, Arrays, Errors).
* **Progressive Rehashing:** The custom hash map implementation prevents latency spikes by gradually migrating keys between an "older" and "newer" table during resizing.
* **Intrusive Data Structures:** Memory efficiency is optimized by using an intrusive approach with the `container_of` macro to link structures, reducing allocation overhead.

## 🧠 Core Data Structures

* **Hash Table:** Used for the top-level database to store and retrieve all keys in average constant time.
* **AVL Tree:** A self-balancing binary search tree used to maintain the heights and subtree sizes (counts) of sorted data.
* **Sorted Sets (ZSet):** Implemented using a dual-structure approach, combining a hash map for rapid score lookups by name, and an AVL tree to order tuples by score for fast range queries.
* **Doubly Linked List:** A custom intrusive doubly linked list (`DList`) used as a queue to manage connection timers, natively ordering them by their last active timestamp for fast expiration.

## 🚀 Supported Commands

### Strings
* `set <key> <value>`: Sets or updates a string value.
* `get <key>`: Retrieves a string value.
* `del <key>`: Deletes a string or sorted set from the database.
* `keys`: Lists all keys currently stored in the database.

### Sorted Sets
* `zadd <zset> <score> <name>`: Adds a new tuple or updates the score of an existing one.
* `zscore <zset> <name>`: Retrieves the score associated with a given name.
* `zrem <zset> <name>`: Removes a tuple from the sorted set.
* `zquery <zset> <score> <name> <offset> <limit>`: Returns a list of tuples starting from a specific score and offset, up to a specified limit.

## 🛠️ Build and Run Instructions

This project requires a modern C++ compiler (like `g++`) and can be run on Linux or macOS.

### 1. Compile the Source Code

Compile the server:
```bash
g++ -O2 server.cpp hashtable.cpp zset.cpp avl.cpp -o server
```
Compile the client:
```bash
g++ -O2 client.cpp -o client
```

### 2. Run the Server
Open a terminal and start the server:
```bash
./server
```

### 3. Interact via the client
Open a second terminal and use a client executable to send commands:
```bash
./client set mykey "Hello World"
./client get mykey
./client zadd my_leaderboard 100 "Alice"
./client zadd my_leaderboard 150 "Bob"
./client zquery my_leaderboard 0 "" 0 10
```
