+++
title = "Dev Update"
description = ""
date = 2022-08-10
updated = 2022-08-10
draft = false
template = "blog/page.html"

[taxonomies]
authors = ["Max Meldrum"]

[extra]
lead = ""
+++

This will be a short blog post giving a dev update on what is going on with Arcon.

For the last few months, efforts have been put into Arcon's new runtime/kernel with a strong focus on the state management aspect. 


## A Non-blocking Async Friendly Runtime

The initial Arcon executor was implemented using [Kompact](https://github.com/kompics/kompact), a hybrid actor + component framework. And then, similarly to other systems, state management was offloaded to a store like [RocksDB](http://rocksdb.org/). While Kompact and RocksDB are excellent libraries by themselves, they are not a great fit for Arcon due to numerous reasons.

**Blocking APIs**
Arcon is an I/O bound system that also needs to manage a set of concurrent tasks in a non-blocking cooperative manner. This is not possible to achieve using synchronous frameworks such as Kompact and RocksDB.

**Decoupled Executor + I/O**
As noted in the following Redpanda blog [post](https://redpanda.com/blog/tpc-buffers), the advances in NVMe disks and Networks have turned the CPU into the new bottleneck of modern storage systems. This has made it essential to couple the CPU scheduler with non-blocking async I/O as a way of maximising CPU utilisation. Also, with a decoupled approach, we have no control over the execution of state compactions as they occur in separate threads. This may cause disturbance to the overall system performance and health.

**Compilation times**
RocksDB is a heavy project to compile, and as it's not native Rust code, it slows down the development process.

The first stage is to implement the runtime for a single core, and this is currently what is in the works.
The executor part will not be implemented from scratch, but here we rely on [glommio](https://github.com/DataDog/glommio), a thread-per-core io-uring async runtime. glommio offers a number of great [primitives](https://docs.rs/glommio/latest/glommio/io/index.html) for building storage solutions on top of io_uring and Direct I/O.

Some other things glommio provides out of the box:

1. Cooperative scheduling
2. Various channels
3. Networking
4. Timers
5. Internal stats (I/O and CPU scheduler related).

Taking a step back and re-architecting Arcon from zero has slowed the overall project progress. 
It has been a great learning process as it has given me the chance to dive into various technologies (e.g., io_uring, Direct I/O, Rust async/await).