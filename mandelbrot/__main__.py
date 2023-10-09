import argparse
import array
import sys

from . import BOUNDS, MandelbrotSet, apply_to_gridarray
from ._xypoints import GridSpec, BasicGridArray, BasicMultiprocessingGridArray
from ._concurrency import CONCURRENCY


def _get_grid(spec, concurrentname):
    if concurrentname == 'multiprocessing-not-shared':
        return BasicGridArray.from_spec(spec)
    if 'multiprocessing' in concurrentname:
        return BasicMultiprocessingGridArray.from_spec(spec)
    else:
        return BasicGridArray.from_spec(spec)


def prep(maxiterations, density, concurrent=None):
    mbs = MandelbrotSet(maxiterations)
    gridspec = GridSpec(*BOUNDS, density)
    grid = _get_grid(gridspec, getattr(concurrent, '_concurrent', ''))
    return mbs, grid


#######################################
# output

def output_text(values, width):
    hit = ' *'
    hit = '\u26aa'
    #hit = '\U0001f7e1'
    miss = '  '

    def iter_lines():
        for rowi in range(width):
            line = ''
            for i in range(rowi*width, rowi*width+width):
                if values[i] == 1:
                    line += hit
                else:
                    line += miss
            yield line

    print('*'*(width*2+4))
    print('*', ' '*(width*2), '*')
    for line in iter_lines():
        print('*', line, '*')
    print('*', ' '*(width*2), '*')
    print('*'*(width*2+4))


def output_image(values, width):
    from PIL import Image

    height = width
    BLACK_AND_WHITE = "1"

    image = Image.new(mode=BLACK_AND_WHITE, size=(width, height))
    image.putdata(values)
    image.show()


OUTPUT = {
    'stdout': output_text,
    'image': output_image,
}


#######################################
# the script

'''
 (with GIL)
 size: 100/100

 threads-not-shared                86s
 threads-shared-direct            105s
 threads-shared-executor           93s

 interpreters-shared-pipe          ---
 interpreters-not-shared-pipe      28s
 interpreters-shared-channel       19s
 interpreters-not-shared-channel   19s

 multiprocessing-not-shared        19s
 multiprocessing-shared-direct     24s
 multiprocessing-shared-executor   ---

 async                             76s
'''

def parse_args(argv=sys.argv[1:], prog=sys.argv[0]):
    parser = argparse.ArgumentParser(prog=prog)

    parser.add_argument('--maxiterations', type=int)
    parser.add_argument('--density', type=int)
    parser.add_argument('--output', choices=list(OUTPUT))
    parser.add_argument('--concurrent', choices=list(CONCURRENCY))

    args = parser.parse_args(argv)
    ns = vars(args)

    if args.concurrent is not None:
        args.concurrent = CONCURRENCY[args.concurrent]

    for key, value in list(ns.items()):
        if value is None:
            del ns[key]

    return ns


def main(maxiterations=100, density=None, output='stdout', concurrent=None):
    if not density:
        if output == 'stdout':
            density = 80
        else:
            density = 512

    try:
        output = OUTPUT[output]
    except KeyError:
        raise ValueError(f'unsupported output {output!r}')

    mbs, grid = prep(maxiterations, density, concurrent)
    apply_to_gridarray(mbs, grid, concurrent)
    output(grid.values, grid.density)


if __name__ == '__main__':
    kwargs = parse_args()
    main(**kwargs)
