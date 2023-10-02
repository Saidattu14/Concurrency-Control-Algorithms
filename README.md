### concurrency control schemes

* Two versions of locking schemes, both of which implement variations of the standard two-phase locking algorithm.
* Serial Optimistic Concurrency Control (Forward validation and Backward validation).
* Parallel Optimistic Concurrency Control (Forward validation and Backward validation).
* Multiversion Timestamp Ordering Concurrency Control.
* Multiversion Concurrency Control Two Phase Locking.


## Commands to run the project

```shell
mkdir build && cmake ..
make -j8
ctest -V
```