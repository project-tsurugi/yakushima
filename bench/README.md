# yakushima benchmark

Benchmarking of yakushima and malloc.

## Preparation

Please do release-build.
If you do benchmarking of yakushima,
you should also build some high performance memory allocator (ex. jemalloc) to avoid contentions against heap memory.

``` shell
cd [/path/to/project_root]
mkdir build-release
cd build-release
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
```

## Running

``` shell
cd [/path/to/project_root]
cd build-release/bench
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima
LD_PRELOAD=[/path/to/some memory allocator lib] ./malloc
```

### note : If you don't use a high performance memory allocator, heap memory contention may result in poor performance

## yakushima : Available options

* `-duration`
  + This is experimental time [seconds].
  + default : `3`
* `-get_initial_record`
  + This is the number of initial key-values for get benchmarking.
  + default : `1000`
* `-get_skew`
  + This is the access zipf skew for get benchmarking.
  + default : `0.0`
* `-instruction`
  + This is the selection of benchmarking.
  + default : `get`
  + Please use `get` or `put`.
* `-thread`
  + This is the number of worker threads.
  + default : `1`
* `-value_size`
  + This is the size of value which is of key-value.
  + default : `4`
  + Please set very small size if you want to check sharpness of parallel logic. Otherwise, if you want to check 

  the realistic performance, you should set appropriate size.

## malloc : Available options

* `-alloc_size`
  + Memory allocation size for bench.
  + default : `4`
* `-duration`
  + This is experimental time [seconds].
  + default : `3`
* `-thread`
  + This is the number of worker threads.
  + default : `1`

  

## yakushima Example

* Get benchmark.
  + duration : `10`
  + get_initial_record : `1000000`
  + get_skew : default : `0.0`
  + instruction : default : `get`
  + thread : `200`
  + value_size : default : `4`

```  shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima -duration 10 -get_initial_record 1000000 -thread 200
```

* Put benchmark.
  + duration : `10`
  + [unused] get_initial_record : default
  + [unused] get_skew : default 
  + instruction : `put`
  + thread : `200`
  + value_size : default : `4`

``` shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima -instruction put -duration 10 -thread 200
```

## malloc Example

* benchmark.
  + alloc_size : `1000`
  + duration : `10`
  + thread : `224`

``` shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./malloc -alloc_size 1000 -duration 10 -thread 224
```
