## Stitchd
Yet another key value store - a work in progress...

Stitchd's API is inspired by tidwall/buntdb and boltdb/bolt and makes use of their elegant API design. Stitchd strives 
to add a feature set that is tailored to a high throughput and less rigidly persistent use case with seamless geolocation support. 

This has also been a great way to dive deeper into the KV/DB world and understand the problems and some of the solutions that 
are currently employed to mitigate those challenges. Hopefully, others will find this project useful if not for use in a project, 
then to learn something from. 

### Goals
Stitchd was born out of a need to replace a legacy timeseries/geolocation package with a more robust real-time solution 
that could standalone as a separate service with little work. It needed to have separation of data or buckets, searchable 
indexes, invalidation, expiration and, custom event callbacks. Additionally, we wanted the operation and code to remain as
light weight and manipulable as possible.
 
### Tradeoffs and Consideraitons
* Fast operation and real-time snapshot of data over hardened and optimized data persistence.
* Geographical functionality built in.
* Easily extensible API and feature set.
* Definable triggers for items, and buckets.
* Ability for items to be expired and invalidated. 
* Minimal "stop-the-world" db/bucket manager executions.
 
### Documentation

The Wiki is full of explanations and examples:

[https://github.com/cbergoon/Stitchd/wiki](https://github.com/cbergoon/Stitchd/wiki)

API documentation is available at:

[http://godoc.org/cbergoon/stitchd](http://godoc.org/cbergoon/stitchd)

Find some other writings about the use case and design of Stitchd below:

[http://cbergoon.github.io/stitchd/a1](http://cbergoon.github.io/stitchd/a1)
[http://cbergoon.github.io/stitchd/a2](http://cbergoon.github.io/stitchd/a2) 

### Usage

There are more extensive examples and how-to's in the resources above but to get your feet wet all you need to do is install Stitchd with: 

```bash 
go get github.com/cbergoon/stitchd
```

Here is some boiler plate code to get started with:  

```go
package main

import (
        "fmt"
        
        "github.com/cbergoon/stitchd"
)

func main(){
        //Todo: Finish the example
}

```

Then run it with:
```bash
go run <filename>.go
```
### Other Stitchd Projects
* [Stitchd-beacon](https://github.com/cbergoon/Stitchd-beacon) - Builds a HTTP API and RPC networking layer over Stitchd allowing it to operate as a standalone service.
* [Stitchd-raft](https://github.com/cbergoon/Stitchd-raft) - An distributed and consistent service that adds RAFT to Stitchd-beacon (work in progress name).
  
### Todo
1. 
2.
