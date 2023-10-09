assert __name__ == '__main__'
from mandelbrot.__main__ import parse_args, main
kwargs = parse_args()
main(**kwargs)
