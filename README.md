# concurrency-benchmarks

I've been running the mandelbrot "benchmark" like this:

`time ../cpython/python mandelbrot-script.py --maxiterations 100 --density 100 --concurrent interpreters-shared-channel'

(For now use my https://github.com/ericsnowcurrently/cpython/tree/benchmark-fixes branch.)

I haven't finished getting the pyperformance-compatible benchmnark working.

TODO:

* Get the pyperformance benchmark working for mandelbrot
* add more benchmarks
