# yakushima benchmark

Benchmarking of yakushima and malloc.

## Preparation

Please do release-build.
If you do benchmarking of yakushima,
you should also build some high performance memory allocator (ex. jemalloc) to avoid contentions against heap memory.

``` shell
cd [/path/to/project_root]
mkdir build_release
cd build_release
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
ninja
```

## Running

``` shell
cd [/path/to/project_root]
cd build_release/bench
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima_bench
LD_PRELOAD=[/path/to/some memory allocator lib] ./malloc_bench
```

### note : If you don't use a high performance memory allocator, heap memory contention may result in poor performance

## `yakushima_bench` : Available options

* `-duration`
  + This is experimental time [seconds].
  + default : `3`
* `-initial_record`
  + This is the number of initial key-values for get / remove benchmarking.
  + note: remove performance is about larger than 553k ops / thread / sec. So
  you should set very large initial_record at remove benchmark.
  + default : `1000`
* `-get_skew`
  + This is the access zipf skew for get benchmarking.
  + default : `0.0`
* `-instruction`
  + This is the selection of benchmarking.
  + default : `get`
  + Please use `get`, `put`, `scan`, or `remove`.
* `-range_of_scan`
  + Number of elements of range scan.
  + default : `1000`
  + Please use `scan`.
* `-thread`
  + This is the number of worker threads.
  + default : `1`
  + the max number can change by invoking cmake with `-DYAKUSHIMA_MAX_PARALLEL_SESSIONS=<num>`.
* `-value_size`
  + This is the size of value which is of key-value.
  + default : `8`
  + Please set very small size if you want to check sharpness of parallel logic. Otherwise, if you want to check

  the realistic performance, you should set appropriate size.

## `malloc_bench` : Available options

* `-alloc_size`
  + Memory allocation size for bench.
  + default : `4`
* `-duration`
  + This is experimental time [seconds].
  + default : `3`
* `-thread`
  + This is the number of worker threads.
  + default : `1`

## `yakushima_bench` : Example

* Get benchmark.
  + duration : default : `3`
  + initial_record : `1000000`
  + get_skew : default : `0.0`
  + instruction : default : `get`
  + thread : `200`
  + value_size : default : `8`

```  shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima_bench -initial_record 1000000 -thread 200
```

* Scan benchmark.
  + duration : default : `3`
  + initial_record : `1000000`
  + get_skew : default : `0.0`
  + instruction : `scan`
  + range_of_scan : `1000` (Use default value)
  + thread : `200`
  + value_size : default : `8`

```  shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima_bench -initial_record 1000000 -thread 200 -instruction scan
```

* Remove benchmark.
  + duration : default : `3`
  + initial_record : `1000000`
  + get_skew : default : `0.0`
  + instruction : `remove`
  + thread : `200`
  + value_size : default : `8`

```  shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima_bench -initial_record 1000000 -thread 200 -instruction remove
```

* Put benchmark.
  + duration : default : `3`
  + [unused] initial_record : default
  + [unused] get_skew : default
  + instruction : `put`
  + thread : `200`
  + value_size : default : `8`

``` shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./yakushima_bench -instruction put -thread 200
```

## `malloc_bench` : Example

* benchmark.
  + alloc_size : `1000`
  + duration : `10`
  + thread : `224`

``` shell
LD_PRELOAD=[/path/to/some memory allocator lib] ./malloc_bench -alloc_size 1000 -duration 10 -thread 224
```
