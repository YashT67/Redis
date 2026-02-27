# Custom In-Memory Data Store (Redis Clone)

A high-performance, asynchronous key-value data store built entirely from scratch in C++. 

Instead of relying on standard libraries, this project implements its own core system internals, including a custom hashtable with progressive rehashing and a non-blocking event loop, designed to handle multiple concurrent client connections efficiently.

## Core Architecture
* **Progressive Rehashing:** Features a custom-built hashtable that migrates data incrementally during resizing, guaranteeing stable $O(1)$ lookups without latency spikes.
* **Asynchronous Event Loop:** Utilizes `poll` and non-blocking sockets to manage multiple client connections concurrently without multithreading overhead.
* **Custom Binary Protocol:** Implements a lightweight, bespoke serialization protocol to parse and frame messages (Strings, Integers, Arrays, Errors) between the client and server.

## Supported Commands
* `set [key] [value]` : Stores a key-value pair.
* `get [key]` : Retrieves the value associated with a key.
* `del [key]` : Deletes a key-value pair.
* `keys` : Returns an array of all currently stored keys.

## How to Build and Run

**1. Compile the Server and Client**
```bash
g++ server.cpp hashtable.cpp -o server
g++ client.cpp -o client
