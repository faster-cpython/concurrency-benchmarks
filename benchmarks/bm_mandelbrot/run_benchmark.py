"""Test the performance of rendering the mandelbrot set.

This workload represents using many tasks that do a small computation
on a small amount of data.
"""

import pyperf

import mandelbrot


# This determines how many complex numbers are analyzed, i.e. DENSITY^2,
# and thus how many tasks there are to perform.
DENSITY = 100
#DENSITY = 300

# For benchmarks with multiple workers, we use as large a number
# as all modes can support.
NUM_WORKERS = 80


def bench_mandelbrot(loops, name):
    mode = name

    mb = mandelbrot.MandelbrotSet(maxiterations=500)
    grid = mandelbrot.Grid(density=DENSITY)
    data = grid.as_array(mp_shared=(mode == 'multiprocessing'))

    t0 = pyperf.perf_counter()
    mandelbrot.apply_to_array(mb, data, mode, NUM_WORKERS)
    t1 = pyperf.perf_counter()

    # XXX Verify the results!
    
    return t1 - t0


BENCHMARKS = [
    'loop',
    'threads',
    'interpreters',
    'multiprocessing',
    'async',
]


if __name__ == "__main__":
    inner_loops = None

    def add_cmdline_args(cmd, args):
        cmd.append(args.benchmark)
    runner = pyperf.Runner(add_cmdline_args=add_cmdline_args)
    runner.metadata['description'] = "Test the performance of rendering the mandelbrot set."

    parser = runner.argparser
    benchmarks = sorted(BENCHMARKS)
    parser.add_argument("benchmark", choices=benchmarks)

    options = runner.parse_args()
    name = options.benchmark

    runner.bench_time_func(name, bench_mandelbrot, name, inner_loops=inner_loops)
