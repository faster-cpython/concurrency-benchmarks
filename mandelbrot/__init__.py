# Inspired by: https://realpython.com/mandelbrot-set-python/

from math import log
import os.path


IMPORT_PATH_ENTRY = os.path.dirname(os.path.dirname(__file__))

BOUNDS = (-2, 0.5, -1.5, 1.5)


def xy_to_complex(x, y):
    return x + y * 1j


def iter_xy_to_complex(points):
    for x, y in points:
        yield x + y * 1j


def apply_to_gridarray(mbs, grid, concurrent=None):
    if concurrent is None:
        values = grid.values
        for i, p in enumerate(grid.iter_points()):
            values[i] = mbs.stability_xy(*p)
    else:
        concurrent(mbs, grid)


class MandelbrotSet:

    def __init__(self, maxiterations, *, escape_radius=2.0, clamp=True):
        self.maxiterations = maxiterations
        self.escape_radius = escape_radius
        self.clamp = clamp

    def __contains__(self, c):
        return self.stability(c, clamp=True) == 1

    def escape_count(self, c, smooth=False):
        z = 0
        for iteration in range(self.maxiterations):
            z = z ** 2 + c
            if abs(z) > self.escape_radius:
                if smooth:
                    return iteration + 1 - log(log(abs(z))) / log(2)
                return iteration
        return self.maxiterations

    def stability(self, c, smooth=None, clamp=None):
        if clamp is None:
            clamp = smooth if smooth is not None else self.clamp
        if smooth is None:
            smooth = False
        value = self.escape_count(c, smooth) / self.maxiterations
        return max(0.0, min(value, 1.0)) if clamp else value

    def stability_xy(self, x, y, smooth=None, clamp=None):
        c = x + y * 1j
        return self.stability(c, smooth, clamp)

    def map_xy(self, points, smooth=None, clamp=None):
        for p in points:
            x, y = p
            c = x + y * 1j
            yield p, self.stability_xy(*p, smooth, clamp)


#############################
# aliases

from ._xypoints import (
    GridSpec,
    GridArray, BasicGridArray,
    MultiprocessingGridArray, BasicMultiprocessingGridArray,
)
