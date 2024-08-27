(For discussion about concurrency, see https://github.com/faster-cpython/concurrency-benchmarks/wiki.)
(For discussion about benchmarking, see https://github.com/faster-cpython/ideas/wiki/All-About-Python-Benchmarking.)

# concurrency-benchmarks

I've been running the mandelbrot "benchmark" like this:

```
$ time ../cpython/python mandelbrot-script.py --maxiterations 100 --density 100 --concurrent interpreters-shared-channel
```

(For now use my https://github.com/ericsnowcurrently/cpython/tree/benchmark-fixes branch.)

I haven't finished getting the pyperformance-compatible benchmnark working.

TODO:

* Get the pyperformance benchmark working for mandelbrot
* add more benchmarks

High-level (workload-oriented) benchmarks to consider:
* [ ] network server
* [ ] map-reduce
* [ ] file/net scanning (e.g. grep, port scanning)
* [ ] text procesing
* [ ] numeric work
* [x] many tasks that do small computation on small amount of data (pixel-by-pixel work on an image)
* [ ] merge-sort (a bunch of dicts)
* [ ] encryption (xor encryption on multi-gigabyte file)
* [ ] image scaling or invert colors
* [ ] decode audio file
* [ ] use cases from https://peps.python.org/pep-0703/#motivation
