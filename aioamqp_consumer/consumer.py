import asyncio
import logging

import async_timeout
from aioamqp import AioamqpException

from .amqp import AMQPMixin
from .exceptions import Ack, DeadLetter, Reject
from .log import logger
from .packer import PackerMixin
from .run import run
from .utils import unpartial


class Consumer(
    PackerMixin,
    AMQPMixin,
):

    _consumer_tag = _queue_info = _consumer_info = None

    exclusive = False

    def __init__(
        self,
        amqp_url,
        task,
        queue_name,
        *,
        debug=False,
        concurrency=1,
        tasks_per_worker=1,
        task_timeout=None,
        reconnect_delay=5.0,
        reject_exceptions=tuple(),
        dead_letter_exceptions=tuple(),
        queue_kwargs=None,
        amqp_kwargs=None,
        packer=None,
        packer_cls=None,
        _no_packer=False,
        marshal_exc=None,
    ):
        if concurrency <= 0:
            raise NotImplementedError

        if queue_kwargs is None:
            queue_kwargs = {}

        if amqp_kwargs is None:
            amqp_kwargs = {}

        self.loop = asyncio.get_event_loop()

        self.amqp_url = amqp_url
        self.task = task
        self.queue_name = queue_name
        self.debug = debug
        self._concurrency = concurrency
        self.tasks_per_worker = tasks_per_worker
        self.task_timeout = task_timeout
        self.reconnect_delay = reconnect_delay

        self.reject_exceptions = reject_exceptions
        self.dead_letter_exceptions = dead_letter_exceptions

        self.queue_kwargs = queue_kwargs
        self.amqp_kwargs = amqp_kwargs

        self._queue = asyncio.Queue()
        self._workers = set()
        self._scale_lock = asyncio.Lock()
        self._stop_lock = asyncio.Lock()
        self._consume_callback_fut = self.loop.create_future()
        self._consume_callback_fut.set_result(None)
        self._down = asyncio.Event()
        self._down.set()
        self._up = asyncio.Event()

        _fn = unpartial(self.task)
        self._task_is_coro = asyncio.iscoroutinefunction(_fn)

        if self.task_timeout is not None and not self._task_is_coro:
            raise NotImplementedError

        super().__init__(
            packer=packer,
            packer_cls=packer_cls,
            _no_packer=_no_packer,
        )

        self.marshal_exc = marshal_exc

        self.__monitor = self.loop.create_task(self._monitor())

    async def _get_concurrency(self):
        async with self._scale_lock:
            return self._concurrency

    def _set_concurrency(self, concurrency):
        raise NotImplementedError

    concurrency = property(_get_concurrency, _set_concurrency)

    @property
    def busy(self):
        return not self._queue.empty()

    @property
    def closed(self):
        return self._closed

    def _on_error_callback(self, exc):
        # TODO: _on_error_callback called twice on close
        # (ChannelClosed(None, None), 'Channel is closed')
        # (AmqpClosedConnection(), 'Channel is closed')

        super()._on_error_callback(exc)
        if not self._down.is_set():
            self._down.set()

        if self._up.is_set():
            self._up.clear()

    def _consume_callback(self, channel, payload, envelope, properties):
        task = payload, envelope, properties

        self._queue.put_nowait(task)

        return self._consume_callback_fut

    def _add_worker(self):
        worker = self.loop.create_task(self._worker())

        self._workers.add(worker)

        worker.add_done_callback(self._workers.remove)

        msg = 'Added worker for queue: %(queue)s. ' \
              'Workers number is %(workers)s'
        context = {
            'queue': self.queue_name,
            'workers': len(self._workers),
        }
        logger.debug(msg, context)

    async def _remove_worker(self):
        worker = next(iter(self._workers))

        worker.cancel()

        try:
            await worker
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            msg = 'Worker (queue: %(queue)%) errored during removing'
            context = {'queue': self.queue_name}
            logger.exception(msg, context, exc_info=exc)
        except AioamqpException as exc:
            msg = 'Worker (queue: %(queue)%) ' \
                  'faced connection problems during removing'
            context = {'queue': self.queue_name}
            logger.debug(msg, context, exc_info=exc)

        msg = 'Removed worker for queue: %(queue)s. ' \
              'Workers number is %(workers)s'
        context = {
            'queue': self.queue_name,
            'workers': len(self._workers),
        }
        logger.debug(msg, context)

    def _log_task(self, status, lvl=logging.DEBUG, exc_info=False):
        msg = 'Task (queue: %(queue)s): %(status)s)'
        context = {
            'queue': self.queue_name,
            'status': status,
        }
        logger.log(lvl, msg, context, exc_info=exc_info)

    async def _run_task(self, payload, properties, delivery_tag):
        try:
            try:
                if self.packer is not None:
                    try:
                        payload = await self.packer.unmarshal(payload)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self._log_task(
                            'unmarshal error',
                            logging.DEBUG,
                            exc_info=exc,
                        )

                        if self.marshal_exc is None:
                            raise

                        raise self.unmarshal_exc from exc

                _task = self._wrap(payload, properties)
            except self.reject_exceptions as exc:
                raise Reject from exc
            except self.dead_letter_exceptions as exc:
                raise DeadLetter from exc

            if not self._task_is_coro:
                raise Ack

            try:
                async with async_timeout.timeout(self.task_timeout) as cm:
                    try:
                        await _task
                    except self.reject_exceptions as exc:
                        raise Reject from exc
                    except self.dead_letter_exceptions as exc:
                        raise DeadLetter from exc
            except asyncio.TimeoutError as exc:
                if not cm.expired:
                    raise

                self._log_task('timeouted', exc_info=exc)
                raise Reject from exc
        except Ack:
            pass
        except DeadLetter as exc:
            self._log_task('dead lettered', logging.WARNING, exc_info=exc)
            return self._basic_reject(delivery_tag, requeue=False)
        except Reject as exc:
            self._log_task('rejected', logging.DEBUG, exc_info=exc)
            return self._basic_reject(delivery_tag, requeue=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log_task('errored', lvl=logging.ERROR, exc_info=exc)
        else:
            self._log_task('successfully finished')

        return self._basic_client_ack(delivery_tag)

    async def _worker(self):
        while True:
            try:
                finalizer = delivery_tag = None
                payload, envelope, properties = await self._queue.get()
                delivery_tag = envelope.delivery_tag

                try:
                    coro = await self._run_task(
                        payload,
                        properties,
                        delivery_tag,
                    )

                    finalizer = self.loop.create_task(coro)

                    await asyncio.shield(finalizer)
                finally:
                    self._queue.task_done()
            except asyncio.CancelledError:
                msg = 'Worker (queue: %(queue)s) is cancelled'
                context = {'queue': self.queue_name}
                logger.debug(msg, context)

                if finalizer is not None:
                    if not finalizer.done():
                        msg = 'Worker (queue: %(queue)s) doing ' \
                              'finalization while cancellation'
                        context = {'queue': self.queue_name}
                        logger.debug(msg, context)
                        await finalizer

                    break

                if self._down.is_set():
                    break

                if delivery_tag is not None:
                    msg = 'Worker (queue: %(queue)s) doing ' \
                          'basic reject due cancellation'
                    context = {'queue': self.queue_name}
                    await self._basic_reject(delivery_tag, requeue=True)

                break

    async def ok(self, timeout=None):
        if self._up.is_set():
            return

        try:
            async with async_timeout.timeout(timeout):
                await self._up.wait()
        except asyncio.TimeoutError as exc:
            raise AioamqpException from exc

    async def scale(self, concurrency, wait_ok=True, timeout=None):
        if concurrency <= 0:
            raise NotImplementedError

        assert not self._closed

        if wait_ok:
            await self.ok(timeout=timeout)

        async with self._scale_lock:
            # for initial scale, compare to real amount of workers
            current_concurrency = len(self._workers)

            msg = 'Scaling workers from %(prev_conc)s to %(next_conc)s'
            context = {
                'prev_conc': current_concurrency,
                'next_conc': concurrency,
            }
            logger.debug(msg, context)

            self._concurrency = concurrency

            if self._concurrency != current_concurrency:
                prefetch = self._concurrency * self.tasks_per_worker
                try:
                    await self._basic_qos(
                        prefetch_size=0,
                        prefetch_count=prefetch,
                        connection_global=True,
                    )
                except AioamqpException as exc:
                    msg = 'Connection problem during consumer scale ' \
                          '(queue: %(queue)s). Not scaled. Scale will be ' \
                          'performed by monitor on next iteration'
                    context = {'queue': self.queue_name}
                    logger.warning(msg, context, exc_info=exc)
                    return

            if current_concurrency > self._concurrency:
                for _ in range(current_concurrency - self._concurrency):
                    await self._remove_worker()
            elif current_concurrency < self._concurrency:
                for _ in range(self._concurrency - current_concurrency):
                    self._add_worker()

            assert len(self._workers) == self._concurrency

        msg = 'Scaled workers from %(prev_conc)s to %(curr_conc)s'
        context = {
            'prev_conc': current_concurrency,
            'curr_conc': self._concurrency,
        }
        logger.debug(msg, context)

        self._after_scale()

    async def join(self, delay=1, timeout=None, wait_ok=True):
        queue_kwargs = self.queue_kwargs.copy()
        queue_kwargs['passive'] = True

        async with async_timeout.timeout(timeout):
            while True:
                if wait_ok:
                    await self.ok()

                await self._queue.join()

                try:
                    await asyncio.sleep(delay)

                    queue = await self._queue_declare(
                        queue_name=self.queue_name,
                        **queue_kwargs,
                    )
                except AioamqpException as exc:
                    msg = 'Connection error during join in consumer ' \
                          '(queue: %(queue)s).'
                    context = {'queue': self.queue_name}
                    logger.warning(msg, context, exc_info=exc)
                    continue
                else:
                    if queue['message_count'] == 0 and self._queue.empty():
                        break

    def _after_connect(self):
        pass

    def _after_consume(self):
        pass

    def _after_scale(self):
        pass

    def _wrap(self, payload, properties):
        return self.task(payload, properties)

    async def _connect(self):
        while True:
            try:
                await super()._connect(self.amqp_url, **self.amqp_kwargs)

                self._queue_info = await self._queue_declare(
                    queue_name=self.queue_name,
                    **self.queue_kwargs,
                )

                self._after_connect()

                await self.scale(self._concurrency, wait_ok=False)

                self._consumer_info = await self._basic_consume(
                    self._consume_callback,
                    self.queue_name,
                    no_ack=False,
                    exclusive=self.exclusive,
                )

                self._consumer_tag = self._consumer_info['consumer_tag']

                self._after_consume()

                msg = 'Consumer (queue: %(queue)s) connected to amqp'
                context = {'queue': self.queue_name}
                logger.debug(msg, context)

                break
            except AioamqpException as exc:
                msg = 'Cannot connect to amqp. Reconnect in %(delay)s seconds'
                context = {'delay': self.reconnect_delay}
                logger.warning(msg, context, exc_info=exc)

                await asyncio.sleep(self.reconnect_delay)

    async def _consume(self):
        await self._connect()

        self._up.set()

        self._down.clear()

        msg = 'Consumer (queue: %(queue)s) started consuming'
        context = {'queue': self.queue_name}
        logger.debug(msg, context)

    async def _stop(self, cancel_workers=True, flush_queue=True):
        async with self._stop_lock:
            if self._up.is_set():
                self._up.clear()

            msg = 'Consumer (queue: %(queue)s) is stopped'
            context = {'queue': self.queue_name}
            logger.debug(msg, context)

            async with self._scale_lock:
                if cancel_workers:
                    self.__cancel_workers()

                if self._workers:
                    await asyncio.gather(*self._workers)

                msg = 'All workers (queue: %(queue)s) are stopped'
                context = {'queue': self.queue_name}
                logger.debug(msg, context)

                if flush_queue:
                    n = 0
                    while self._queue.qsize():
                        self._queue.get_nowait()
                        self._queue.task_done()
                        n += 1

                    msg = 'Flushed from internal (queue: %(queue)s) %(n)i messages'  # noqa
                    context = {
                        'queue': self.queue_name,
                        'n': n,
                    }
                    logger.debug(msg, context)

            await self._disconnect()

    def __cancel_workers(self):
        msg = 'Stopping consumer workers (queue: %(queue)s)'
        context = {'queue': self.queue_name}
        logger.debug(msg, context)

        for worker in self._workers:
            assert not worker.cancelled(), 'Stopped worker detected'

            worker.cancel()

    async def _monitor(self):
        while True:
            try:
                await self._consume()

                await self._down.wait()

                await self._stop()
            except asyncio.CancelledError:
                break

    async def _disconnect(self):
        if self._transport is not None and self._protocol is not None:
            if self._channel is not None:
                if self._consumer_tag is not None:
                    try:
                        await self._basic_cancel(self._consumer_tag)
                    except AioamqpException:
                        pass

                    msg = 'Consumer (queue: %(queue)s) stopped consuming'
                    context = {'queue': self.queue_name}
                    logger.debug(msg, context)

        self._consumer_tag = self._queue_info = self._consumer_info = None

        await super()._disconnect()

        msg = 'Consumer (queue: %(queue)s) disconnected from amqp'
        context = {'queue': self.queue_name}
        logger.debug(msg, context)

    def close(self):
        assert not self._closed, 'Already closed'

        self._closed = True

        self.__monitor.cancel()
        self.__cancel_workers()

        msg = 'Consumer (queue: %(queue)s) is closing'
        context = {'queue': self.queue_name}
        logger.debug(msg, context)

    def run(self, **kwargs):
        kwargs['debug'] = kwargs.pop('debug', self.debug)
        kwargs['loop'] = self.loop
        run(self, **kwargs)

    async def wait_closed(self):
        assert self._closed, 'Must be closed first'

        try:
            await self.__monitor
        except asyncio.CancelledError:
            pass

        await self._stop(cancel_workers=False)

        await self._consume_callback_fut

        msg = 'Consumer (queue: %(queue)s) was closed'
        context = {'queue': self.queue_name}
        logger.debug(msg, context)

    async def __aenter__(self):
        await self.ok()
        return self

    async def __aexit__(self, *exc_info):
        self.close()
        await self.wait_closed()
