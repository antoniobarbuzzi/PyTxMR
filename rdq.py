import time

from twisted.internet import defer, task
from twisted.python import log


class DeferredPool(object):
    def __init__(self, initialContents=None):
        self._pool = set()
        self._waiting = []
        if initialContents:
            for d in initialContents:
                self.add(d)

    def _fired(self, result, d):
        self._pool.remove(d)
        if not self._pool:
            waiting, self._waiting = self._waiting, []
            for waiter in waiting:
                waiter.callback(None)
        return result

    def add(self, d):
        d.addBoth(self._fired, d)
        self._pool.add(d)
        return d

    def deferUntilEmpty(self, testImmediately=True):
        if testImmediately and not self._pool:
            return defer.succeed(None)
        else:
            d = defer.Deferred()
            self._waiting.append(d)
            return d


class QueueStopped(Exception):
    pass


class Underway(object):
    def __init__(self, job, deferred):
        self.job = job
        self.deferred = deferred
        self.startTime = time.time()


class ResizableDispatchQueue(object):

    _nop = object()
    
    def __init__(self, func, width=0, size=None, backlog=None):
        self._queue = defer.DeferredQueue(size, backlog)
        self._func = func
        self._pool = DeferredPool()
        self._coop = task.Cooperator()
        self._currentWidth = 0
        self._pendingStops = 0
        self._underway = set()
        self.stopped = self.paused = False
        self.width = int(width)
        assert self.width >= 0

    def put(self, obj):
        if self.stopped:
            raise QueueStopped()
        self._queue.put(obj)
        
    def pending(self):
        return self._queue.pending
        
    def underway(self):
        return self._underway

    def clearQueue(self):
        self._queue.pending = []
        
    def size(self):
        return (len(self._underway), len(self._queue.pending))
        
    def _drain(self):
        # Flush idle dispatchers by giving them a nop.
        def _checkEmpty(result):
            assert not self._underway
            log.msg('All tasks have finished.')
            return result
        assert self.paused or self.stopped
        while self._queue.waiting:
            self._queue.put(self._nop)
        d = self._pool.deferUntilEmpty()
        d.addCallback(_checkEmpty)
        return d
        
    def stop(self):
        log.msg('Stopping.')
        self.stopped = True
        self.width = 0
        d = self._drain()
        d.addCallback(lambda _: self.pending())
        return d
        
    def pause(self):
        log.msg('Pausing.')
        self._pausedWidth = self.width
        self.width = 0
        self.paused = True
        return self._drain()
        
    def resume(self, width=None):
        if self.stopped:
            raise QueueStopped()
        log.msg('Resuming.')
        if width is None:
            width = self._pausedWidth
        else:
            width = int(width)
            assert width >= 0
        self.paused = False
        self.width = width
    
    def _call(self, obj):
        if not obj is self._nop:
            d = defer.maybeDeferred(self._func, obj)
            underway = Underway(obj, d)
            self._underway.add(underway)
            def _done(result, item):
                self._underway.remove(item)
                return result
            d.addBoth(_done, underway)
            return d
    
    def next(self):
        if self._pendingStops:
            self._pendingStops -= 1
            self._currentWidth -= 1
            raise StopIteration
        else:
            d = self._queue.get()
            d.addCallback(self._call)
            return d

    def getWidth(self):
        if self.paused:
            return self._pausedWidth
        else:
            return self._currentWidth - self._pendingStops

    def setWidth(self, width):
        width = int(width)
        assert width >= 0
        if self.paused:
            self._pausedWidth = width
        else:
            targetWidth = self._currentWidth - self._pendingStops
            extra = width - targetWidth
            if extra > 0:
                # Make ourselves wider.
                delta = extra - self._pendingStops
                if delta >= 0:
                    self._pendingStops = 0
                    for i in xrange(delta):
                        self._pool.add(self._coop.coiterate(self))
                    self._currentWidth += delta
                else:
                    self._pendingStops -= extra
            elif extra < 0:
                # Make ourselves narrower.
                self._pendingStops -= extra
                
    width = property(getWidth, setWidth)
