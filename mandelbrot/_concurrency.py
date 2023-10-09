import os.path
import queue
import threading

from . import IMPORT_PATH_ENTRY, apply_to_gridarray, MandelbrotSet


#############################
# threads

def mode_threads_not_shared(mbs, grid, num_threads=80):
    values = grid.values
    tasks = queue.Queue()
    results = queue.Queue()

    # Start the results thread.
    done = False
    def handle_results():
        while not done:
            try:
                i, value = results.get(timeout=0.1)
            except queue.Empty:
                continue
            values[i] = value
            results.task_done()
    t_results = threading.Thread(target=handle_results)
    t_results.start()

    # Start the workers.
    def run_worker(grid):
        while not done:
            try:
                #i, x, y = tasks.get(timeout=0.1)
                i = tasks.get(timeout=0.1)
            except queue.Empty:
                continue
            x, y = grid.get_point(i)
            value = mbs.stability_xy(x, y)
            results.put((i, value))
            tasks.task_done()
    threads = [threading.Thread(target=run_worker, args=(grid.spec.copy(),))
               for _ in range(num_threads or 80)]
    for t in threads:
        t.start()

    # Queue up all the tasks.
    for i in range(len(grid)):
        #x, y = grid.look_up_point(i)
        #tasks.put((i, x, y))
        tasks.put(i)
    tasks.join()
    results.join()
    done = True

    # Wait for the workers to finish.
    for t in threads:
        t.join()
    t_results.join()


def mode_threads_shared_direct(mbs, grid, num_threads=80):
    values = grid.values
    tasks = queue.Queue()

    # Start the workers.
    done = False
    def run_worker():
        while not done:
            try:
                index = tasks.get(timeout=0.1)
            except queue.Empty:
                continue
            x, y = grid.look_up_point(index)
            values[index] = mbs.stability_xy(x, y)
            tasks.task_done()
    threads = [threading.Thread(target=run_worker)
               for _ in range(num_threads or 80)]
    for t in threads:
        t.start()

    # Queue up all the tasks.
    for i in range(len(grid)):
        tasks.put(i)
    tasks.join()
    done = True

    # Wait for the workers to finish.
    for t in threads:
        t.join()


def mode_threads_shared_executor(mbs, grid, num_threads=80):
    from concurrent.futures import ThreadPoolExecutor

    values = grid.values

    def task(i):
        x, y = grid.look_up_point(i)
        values[i] = mbs.stability_xy(x, y)

    with ThreadPoolExecutor(max_workers=num_threads or 80) as e:
        e.map(task, range(len(grid)))


#############################
# multiple interpreters

def mode_interpreters_not_shared_channel(mbs, grid, num_interpreters=20):
    from test.support import interpreters
    import _xxinterpchannels as _channels

    values = grid.values

    # Create the task and results queues.
    tasks_r, tasks_s = interpreters.create_channel()
    results_r, results_s = interpreters.create_channel()
    running = queue.Queue()

    # Start the results thread.
    def handle_results():
        while True:
            try:
                msg = results_r.recv()
                #i, res = results_r.recv()
            except _channels.ChannelClosedError:
                break
            i, value = msg.split(':')
            values[int(i)] = float(value)
            running.get(block=False)
            running.task_done()
    t_results = threading.Thread(target=handle_results)
    t_results.start()

    # Start the workers.
    def run_worker():
        interp = interpreters.create()
        interp.run(f'''if True:
            from test.support import interpreters
            import sys
            sys.path.insert(0, {IMPORT_PATH_ENTRY!r})
            from mandelbrot import MandelbrotSet, GridSpec

            ch_tasks = interpreters.RecvChannel(tasks)
            ch_results = interpreters.SendChannel(results)

            mbs = MandelbrotSet({mbs.maxiterations})
            grid = GridSpec(*{tuple(grid.spec)})

            while True:
                try:
                    i = ch_tasks.recv()
                except interpreters.ChannelClosedError:
                    break
                i = int(i)
                    
                x, y = grid.get_point(i)
                value = mbs.stability_xy(x, y)
                ch_results.send(f'{{i}}:{{value}}')
            ''',
            channels=dict(tasks=tasks_r.id, results=results_s.id))
    threads = [threading.Thread(target=run_worker)
               for _ in range(num_interpreters or 20)]
    for t in threads:
        t.start()

#    # Queue up all the tasks.
#    for i in range(len(grid)):
#        tasks_s.send_nowait(i)
#        running.put(i)
#    for i in range(num_interpreters):
#        tasks_s.send_nowait(None)
#    #for i in range(num_interpreters):
#    #    tasks_s.send(None)
#
#    # Wait for the workers to finish.
#    for t in threads:
#        t.join()
#    running.join()
#    #tasks_s.close()
#    _channels.close(tasks_s.id)
#    _channels.close(results_r.id)
#    t_results.join()
#
#    # Make sure all tasks were completed.
#    empty = 'empty-channel'
#    actual = tasks_r.recv_nowait(empty)
#    assert actual is empty, repr(actual)
#    #actual = results_r.recv_nowait(empty)
#    #assert actual is empty, repr(actual)

    # Queue up all the tasks.
    for i in range(len(grid)):
        tasks_s.send_nowait(i)
        running.put(i)
    tasks_s.close()

    # Wait for all the tasks to be done.
    running.join()

    # Stop all threads and let them finish.
#    _channels.close(tasks_s.id)
    tasks_r.close()
#    _channels.close(results_r.id)
    results_r.close()
    results_s.close()
    for t in threads:
        t.join()
    t_results.join()


def mode_interpreters_shared_channel(mbs, grid, num_interpreters=20):
#    num_interpreters = 1
    from test.support import interpreters
    import _xxinterpchannels as _channels

    values = grid.values

    # Create the task queue.
    tasks_r, tasks_s = interpreters.create_channel()
    values_r, values_s = interpreters.create_channel()

    # Start the workers.
    def run_worker():
        interp = interpreters.create()
#        interp.run(f'''if True:
#            from test.support import interpreters
#            from mandelbrot import MandelbrotSet, GridSpec
#
#            values = ch_values.recv()
#            mbs = MandelbrotSet({mbs.maxiterations})
#            grid = GridSpec(*{tuple(grid.spec)})
#
#            while True:
#                # XXX Work on a span of entries to avoid cache collisions.
#                try:
#                    i = ch_tasks.recv()
#                except interpreters.ChannelClosedError:
#                    break
#                i = int(i)
#                    
#                x, y = grid.get_point(i)
#                values[i] = mbs.stability_xy(x, y)
#            ''',
#            channels=dict(ch_tasks=tasks_r, ch_values=values_r))

        def script():
            from test.support import interpreters
            from mandelbrot import MandelbrotSet, GridSpec

            values = ch_values.recv()
            mbs = MandelbrotSet(maxiterations)
            global _gridspec
            _gridspec = eval(_gridspec)
            grid = GridSpec(*_gridspec)

            def fail():
                def error():
                    raise Exception('spam')
                    #raise Exception
                error()
            #fail()

            while True:
                try:
                    i = ch_tasks.recv()
                except interpreters.ChannelClosedError:
                    break
                i = int(i)
                    
                # XXX Work on a span of entries to avoid cache collisions.
                x, y = grid.get_point(i)
                values[i] = mbs.stability_xy(x, y)

        interp.run(script, channels=dict(
                   ch_tasks=tasks_r,
                   ch_values=values_r,
                   maxiterations=mbs.maxiterations,
                   _gridspec=str(tuple(grid.spec)),
                   #_gridspec=tuple(grid.spec),
                   ))
    threads = [threading.Thread(target=run_worker)
               for _ in range(num_interpreters or 20)]
    for t in threads:
        t.start()

    # Send the shared data to each interpreter.
    for i in range(num_interpreters):
        values_s.send_buffer_nowait(values)
    values_s.close()

    # Queue up all the tasks.
    for i in range(len(grid)):
        tasks_s.send_nowait(i)
    tasks_s.close()

    # Wait for the workers to finish.
    for t in threads:
        t.join()

    # Make sure all tasks were completed.
    completed = True
    try:
        while True:
            tasks_r.recv_nowait()
            completed = False
    except interpreters.ChannelClosedError:
        pass
    assert completed
    assert tasks_r.is_closed, tasks_r._info


def mode_interpreters_not_shared_pipe(mbs, grid, num_interpreters=20):
    import os
    from test.support import interpreters

    values = grid.values

    # Create the task and results queues.
    tasks_r, tasks_s = os.pipe()
    football_r, football_s = os.pipe()
    results_r, results_s = os.pipe()
    running = queue.Queue()

    #log = (lambda *a, **k: print(f'   ', *a, **k))
    log = (lambda *a, **k: None)

    # Start the results thread.
    def handle_results():
        #results = os.fdopen(results_r)
        while True:
            msg = ''
            while True:
                try:
                    #c = results.read(1)
                    c = os.read(results_r, 1).decode('utf-8')
                except OSError:
                    log('results:         done', flush=True)
                    return
                if c == ';':
                    break
                msg += c
            log('results:          received', msg, flush=True)
            i, value = msg.split(':')
            values[int(i)] = float(value)
            running.get(block=False)
            running.task_done()
    t_results = threading.Thread(target=handle_results)
    t_results.start()

    # Start the workers.
    def run_worker(i):
        interp = interpreters.create()
        interp.run(f'''if True:
            import os
            import time

            import sys
            sys.path.insert(0, {IMPORT_PATH_ENTRY!r})
            from mandelbrot import MandelbrotSet, GridSpec

            mbs = MandelbrotSet({mbs.maxiterations})
            grid = GridSpec(*{tuple(grid.spec)})

            tasks_fd = {tasks_r}
            results_fd = {results_s}

            workerid = {i}
            #log = (lambda *a, **k: print(f'{{workerid:>3}}', *a, **k))
            log = (lambda *a, **k: None)
            ''')
        interp.run(f'''if True:
            #tasks = os.fdopen({tasks_r})
            #results = os.fdopen({results_s}, 'w')
            try:
                while True:
                    log('waiting for task', flush=True)
                    football = os.read({football_r}, 1)
                    assert football != b''
                    #msg = tasks.read(1)
                    msg = os.read(tasks_fd, 1).decode('utf-8')
                    #assert msg != ';', msg
                    if msg == ';':
                        continue
                    if msg == '^':
                        os.write({football_s}, football)
                        log('tasks:   done', flush=True)
                        break
                    if msg == '':
                        os.write({football_s}, football)
                        log('tasks:   EOF', flush=True)
                        break
                    while True:
                        #c = tasks.read(1)
                        c = os.read(tasks_fd, 1).decode('utf-8')
                        assert c != '^'
                        if c == ';':
                            os.write({football_s}, football)
                            break
                        msg += c
                    i = int(msg)
                    log('tasks:   received         ', i, flush=True)
                        
                    x, y = grid.get_point(i)
                    value = mbs.stability_xy(x, y)
                    log('results:          sending ', i, value, flush=True)
                    res = f'{{i}}:{{value}};'
                    try:
                        #results.write(res)
                        #results.flush()
                        os.write(results_fd, res.encode('utf-8'))
            #            os.fsync(results_fd)
                    except OSError:
                        log('unexpectedly closed', flush=True)
                        raise  # re-raise
            except OSError:
                log('tasks:   done', flush=True)
                pass
            #finally:
            #    tasks.close()
            #    results.close()
            ''')
    threads = [threading.Thread(target=run_worker, args=(i,))
               for i in range(num_interpreters or 20)]
    for t in threads:
        t.start()

    # Queue up all the tasks.
    #tasks = os.fdopen(tasks_s, 'w')
    for i in range(len(grid)):
#        log('tasks:   sending          ', i, flush=True)
        #tasks.write(f'{i};')
        #tasks.flush()
        os.write(tasks_s, f'{i};'.encode('utf-8'))
#        os.fsync(tasks_s)
        running.put(i)
    for i in range(num_interpreters):
        log('tasks:   sending           ^', flush=True)
        #tasks.write('^')
        #tasks.flush()
        os.write(tasks_s, b'^')
#        os.fsync(tasks_s)
    os.write(football_s, b'*')
    log('tasks:   sent', flush=True)

    # Wait for the workers to finish.
    running.join()
    os.close(tasks_r)
    os.close(tasks_s)
    os.close(football_r)
    os.close(football_s)
    os.close(results_r)
    os.close(results_s)
    for t in threads:
        t.join()
    t_results.join()

    # Make sure all tasks were completed.
    return
    empty = 'empty-channel'
    actual = tasks_r.recv_nowait(empty)
    assert actual is empty, repr(actual)
    actual = results_r.recv_nowait(empty)
    assert actual is empty, repr(actual)


def mode_interpreters_shared_pipe(mbs, grid, num_interpreters=20):
    raise NotImplementedError  # XXX
    from test.support import interpreters
    import _xxinterpchannels as _channels

    # Create the task queue.
    tasks_r, tasks_s = interpreters.create_channel()

    # Start the workers.
    def run_worker():
        interp = interpreters.create()
        interp.run(f'''if True:
            import _xxinterpchannels as _channels
            import sys
            sys.path.insert(0, {IMPORT_PATH_ENTRY!r})
            from mandelbrot import MandelbrotSet, GridSpec

            mbs = MandelbrotSet({mbs.maxiterations})
            grid = GridSpec(*{tuple(grid.spec)})

            while True:
                i = tasks.recv()
                if i is None:
                    break
                i = int(i)
                    
                x, y = grid.get_point(i)
                values[i] = mbs.stability_xy(x, y)
            ''',
            channels=dict(tasks=tasks_r, values=values))
    threads = [threading.Thread(target=run_worker)
               for _ in range(num_interpreters or 20)]
    for t in threads:
        t.start()

    # Queue up all the tasks.
    for i in range(len(grid)):
        tasks_s.send_nowait(i)
    for i in range(num_interpreters):
        tasks_s.send_nowait(None)
    #for i in range(len(grid)):
    #    tasks_s.send(i)
    #tasks_s.close()

    # Wait for the workers to finish.
    for t in threads:
        t.join()

    # Make sure all tasks were completed.
    empty = 'empty-channel'
    actual = tasks_r.recv_nowait(empty)
    assert actual is empty, repr(actual)


#############################
# multiprocessing

def mode_multiprocessing_not_shared(mbs, grid, num_procs=20):
    from multiprocessing import Process, JoinableQueue
    from ._xypoints import MultiprocessingGridArray

    assert not isinstance(grid, MultiprocessingGridArray), type(grid)
    values = grid.values
    tasks = JoinableQueue()
    results = JoinableQueue()

    # Start the results thread.
    done = False
    def handle_results():
        while not done:
            try:
                i, value = results.get(timeout=0.1)
            except queue.Empty:
                continue
            values[i] = value
            results.task_done()
    t_results = threading.Thread(target=handle_results)
    t_results.start()

    # Start the workers.
    def run_worker(grid, maxiterations):
        mbs = MandelbrotSet(maxiterations)
        while True:
            try:
                #i, x, y = tasks.get(timeout=0.1)
                i = tasks.get(timeout=0.1)
            except queue.Empty:
                continue
            if i is None:
                tasks.task_done()
                break
            x, y = grid.get_point(i)
            value = mbs.stability_xy(x, y)
            results.put((i, value))
            tasks.task_done()
    procs = [Process(target=run_worker, args=(grid.spec.copy(), mbs.maxiterations))
             for _ in range(num_procs or 20)]
    for p in procs:
        p.start()

    # Queue up all the tasks.
    for i in range(len(grid)):
        tasks.put(i)
    for _ in procs:
        tasks.put(None)
    tasks.join()
    results.join()
    done = True

    # Wait for the workers to finish.
    for p in procs:
        p.join()
    t_results.join()


def mode_multiprocessing_shared_direct(mbs, grid, num_procs=20):
    import multiprocessing
    from ._xypoints import MultiprocessingGridArray

    assert isinstance(grid, MultiprocessingGridArray), type(grid)
    values = grid.values
    tasks = multiprocessing.JoinableQueue()

    # Start the workers.
    def run_worker(values, grid, maxiterations):
        mbs = MandelbrotSet(maxiterations)
        while True:
            try:
                #i, x, y = tasks.get(timeout=0.1)
                i = tasks.get(timeout=0.1)
            except queue.Empty:
                continue
            if i is None:
                tasks.task_done()
                break
            x, y = grid.get_point(i)
            values[i] = mbs.stability_xy(x, y)
            tasks.task_done()
    args = (values, grid.spec.copy(), mbs.maxiterations)
    procs = [multiprocessing.Process(target=run_worker, args=args)
             for _ in range(num_procs or 20)]
    for p in procs:
        p.start()

    # Queue up all the tasks.
    for i in range(len(grid)):
        tasks.put(i)
    for _ in procs:
        tasks.put(None)
    tasks.join()

    # Wait for the workers to finish.
    for p in procs:
        p.join()


def mode_multiprocessing_shared_executor(mbs, grid, num_procs=20):
    raise NotImplementedError  # XXX
    from concurrent.futures import ProcessPoolExecutor
    import multiprocessing
    from ._xypoints import MultiprocessingGridArray

    assert isinstance(grid, MultiprocessingGridArray), type(grid)
    values = grid.values

    def init(values):
        global mp_shard_executor_values
        mp_shard_executor_values = values
    kwargs = dict(initializer=init, initargs=values)

    def task(i):
        x, y = grid.look_up_point(i)
        mp_shard_executor_values[i] = mbs.stability_xy(x, y)

    with ProcessPoolExecutor(max_workers=num_procs or 20, **kwargs) as e:
        e.map(task, range(len(grid)))


#############################
# async

def mode_async(mbs, grid, num_coroutines=80):
    import asyncio

    if not num_coroutines:
        num_coroutines = 80

    values = grid.values

    async def run_worker(tasks):
        while True:
            try:
                i = tasks.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
                continue
            if i is None:
                tasks.task_done()
                break
            x, y = grid.spec.get_point(i)
            values[i] = mbs.stability_xy(x, y)
            tasks.task_done()

    async def run_tasks(tasks):
        for i in range(len(grid)):
            await tasks.put(i)
        # XXX Use task.cancel() on the list of coroutines?
        for _ in range(num_coroutines):
            await tasks.put(None)
        await tasks.join()

    async def main():
        tasks = asyncio.Queue()
        await asyncio.gather(
            run_tasks(tasks),
            *[run_worker(tasks) for _ in range(num_coroutines)],
        )
    asyncio.run(main())


#############################
# the registry

CONCURRENCY = {
    # threads
    'threads': 'threads-not-shared',
    'threads-not-shared': mode_threads_not_shared,
    'threads-shared': 'threads-shared-direct',
    'threads-shared-direct': mode_threads_shared_direct,
    'threads-shared-executor': mode_threads_shared_executor,
    # interpreters
    'interpreters': 'interpreters-not-shared',
    'interpreters-not-shared': 'interpreters-not-shared-pipe',
    'interpreters-shared': 'interpreters-shared-pipe',
    'interpreters-shared-pipe': mode_interpreters_shared_pipe,
    'interpreters-not-shared-pipe': mode_interpreters_not_shared_pipe,
    'interpreters-shared-channel': mode_interpreters_shared_channel,
    'interpreters-not-shared-channel': mode_interpreters_not_shared_channel,
    # multiprocessing
    'multiprocessing': 'multiprocessing-not-shared',
    'multiprocessing-not-shared': mode_multiprocessing_not_shared,
    'multiprocessing-shared': 'multiprocessing-shared-direct',
    'multiprocessing-shared-direct': mode_multiprocessing_shared_direct,
    'multiprocessing-shared-executor': mode_multiprocessing_shared_executor,
    # async
    'async': mode_async,
}
for n, f in list(CONCURRENCY.items()):
    while isinstance(f, str):
        f = CONCURRENCY[n] = CONCURRENCY[f]
    f._concurrent = n
del f, n
CONCURRENCY['loop'] = None
