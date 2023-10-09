import array
from collections import namedtuple


# (x, y) -> scalar  (with colormap)
# (x, y) -> (R, G, B)  (float or 8-bit int)


class GridSpec(namedtuple('GridSpec', 'xmin xmax ymin ymax density')):

    @classmethod
    def parse(cls, text):
        xmin, xmax, ymin, ymax, density = text.strip().split(':')
        return cls(
            float(xmin),
            float(xmax),
            float(ymin),
            float(ymax),
            int(density),
        )

    def __init__(self, *args, **kwargs):
        self._validate()

        xmin, xmax, ymin, ymax, density = self
        self._xstep = (xmax - xmin) / density
        self._ystep = (ymax - ymin) / density

    def _validate(self):
        ...

    def __str__(self):
        return ':'.join(str(v) for v in self)

    @property
    def width(self):
        return self.density

    @property
    def height(self):
        return self.density

    @property
    def count(self):
        return self.width * self.height

    def _get_axes(self):
        xmin, xmax, ymin, ymax, density = self
        xstep = self._xstep
        ystep = self._ystep
        xaxis = [xmin + i * xstep for i in range(self.width-1)] + [xmax]
        yaxis = [ymin + i * ystep for i in range(self.height-1)] + [ymax]
        return xaxis, yaxis

    def iter_rows(self):
        xaxis, yaxis = self._get_axes()
        for y in yaxis:
            yield ((x, y) for x in xaxis)

    def iter_columns(self):
        xaxis, yaxis = self._get_axes()
        for x in xaxis:
            yield ((x, y) for y in yaxis)

    def iter_points(self, *, byrow=True):
        for group in self.iter_rows() if byrow else self.iter_columns():
            yield from group

    def get_point(self, index, *, byrow=True):
        # XXX Calculate it.
        for i, p in enumerate(self.iter_points(byrow=byrow)):
            if i == index:
                return p
        raise IndexError(index)

    def get_index(self, x, y, *, byrow=True):
        # XXX Calculate it.
        for i, p in enumerate(self.iter_points(byrow=byrow)):
            if p[0] == x and p[1] == y:
                return i
        raise IndexError((x, y))

    def copy(self):
        return type(self)(*self)


class _GridArray:
    # This has all the parts that are unlikely to be overridden by subclasses.

    SIZES = {tc: array.array(tc).itemsize for tc in array.typecodes if tc != 'u'}

    # XXX Align to cache line to avoid thrashing when threading.
    CACHE_LINE_BYTES = 64

    @classmethod
    def from_spec(cls, spec, typespec='d'):
        typespec = cls._parse_typespec(typespec)

        data = cls._new_data(spec, typespec)

        self = cls(spec, typespec, data)
        return self

    @classmethod
    def _parse_typespec(cls, typespec):
        if len(typespec) == 0:
            raise ValueError('got empty typespec')
        elif isinstance(typespec, str):
            typespec = tuple(typespec)
            for tc in typespec:
                if tc not in cls.SIZES:
                    raise ValueError(f'unsupported typecode {tc!r}')
        else:
            typespec = tuple(typespec)
            for tc in typespec:
                if tc not in cls.SIZES or not isinstance(tc, str):
                    raise ValueError(f'unsupported typecode {tc!r}')
        return typespec

    def __init__(self, spec, typespec, data):
        self._spec = spec
        self._typespec = typespec
        self._data = self._init_data(data)

    def __repr__(self):
        return f'{type(self).__name__}(spec={self._spec!r}, typespec={self._typespec!r}, data={self._data!r})'

    def __getattr__(self, name):
        if name.startswith('_') or name == 'copy':
            raise AttributeError(name)
        return getattr(self._spec, name)

    def __len__(self):
        return self._spec.count

    @property
    def spec(self):
        return self._spec

    @property
    def typespec(self):
        return self._typespec

    @property
    def ndim(self):
        return 2 + len(self._typespec)


class GridArray(_GridArray):
    # This has all the parts that may/must be overridden by subclasses.

    @classmethod
    def _new_data(cls, spec, typespec):
        raise NotImplementedError

    @classmethod
    def _new_array(cls, typecode, initializer):
        if isinstance(initializer, int):
            initializer = (0 for _ in range(initializer))
        return array.array(typecode, initializer)

    def _init_data(self, data):
        return data

    def __iter__(self):
        yield from self.iter_values()

    def __getitem__(self, index):
        return self.look_up_value(index)

    @property
    def values(self):
        raise NotImplementedError

    def look_up_value(self, index):
        raise NotImplementedError

    def iter_values(self):
        for i in range(self._spec.count):
            yield self.look_up_value(i)

    def look_up_point(self, index):
        # XXX Look it up?
        return self._spec.get_point(index)

    def iter_points(self):
        # XXX Use self.spec.iter_points()?
        for i in range(len(self)):
            yield self.look_up_point(i)

    def find_point(self, x, y):
        return self._spec.get_index(x, y)


class BasicGridArray(GridArray):

    @classmethod
    def _parse_typespec(cls, typespec):
        typespec = super()._parse_typespec(typespec)
        # Only 1-dimensional values are supported, for now.
        if len(typespec) != 1:
            raise ValueError(f'bad typespec {typespec!r}')
        return typespec

    @classmethod
    def _new_data(cls, spec, typespec):
        typecode, = typespec
        return cls._new_array(typecode, spec.count)

    def _init_data(self, data):
        self._values = data
        return data

    def iter_values(self):
        yield from self._values

    def look_up_value(self, index):
        return self._values[index]

    @property
    def values(self):
        return self._values


class CombinedGridArray(GridArray):

    @classmethod
    def _parse_typespec(cls, typespec):
        typespec = super()._parse_typespec(typespec)
        # Only 1-dimensional values are supported, for now.
        if len(typespec) != 1:
            raise ValueError(f'bad typespec {typespec!r}')
        return typespec

    @classmethod
    def _new_data(cls, spec, typespec):
        typecode, = typespec
        size = 3 * spec.count
        data = cls._new_array(typecode, size)
        for i, p in enumerate(spec.iter_points()):
            x, y = p
            st = i * 3
            data[st] = x
            data[st+1] = x
        return data

    def look_up_value(self, index):
        return self._data[index*3 + 2]

    @property
    def values(self):
        return memoryview(self._data)[2::3]


class MultiprocessingGridArray(GridArray):

    @classmethod
    def _new_array(cls, typecode, initializer):
        import multiprocessing
        return multiprocessing.Array(typecode, initializer)


class BasicMultiprocessingGridArray(MultiprocessingGridArray, BasicGridArray):
    pass


class CombinedMultiprocessingGridArray(MultiprocessingGridArray, CombinedGridArray):
    pass


# XXX finish
class SplitGridArray(GridArray):
    # (xy, values)

    @classmethod
    def _new_data(cls, spec, typespec):
        typecode, = typespec
        points = cls._new_array(typecode, 2 * spec.count)
        for i, p in enumerate(spec.iter_points()):
            x, y = p
            st = i * 2
            points[st] = x
            points[st+1] = x
        values = cls._new_array(typecode, spec.count)
        return (points, values)

    def _init_data(self, data):
        self._points, self._values = data
        return data

    def look_up_value(self, index):
        return self._values[index]


# XXX finish
class SplitPointGridArray(GridArray):
    # (x, y, values)
    ...
