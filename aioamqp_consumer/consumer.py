import asyncio
import logging

import aioamqp
import async_timeout

from .exceptions import Ack, DeadLetter, Reject
from .log import logger
from .mixins import AMQPMixin
from .utils import unpartial


class Consumer(AMQPMixin):

    _consumer_tag = None

    def __init__(
        self,
        amqp_url,
        task,
        queue_name,
        *,
        concurrency=1,
        tasks_per_worker=3,
        task_timeout=None,
        reconnect_delay=5.0,
        reject_exceptions=tuple(),
        dead_letter_exceptions=tuple(),
        queue_kwargs=None,
        amqp_kwargs=None,
        loop=None
    ):
        if concurrency <= 0:
            raise NotImplementedError

        if queue_kwargs is None:
            queue_kwargs = {}

        if amqp_kwargs is None:
            amqp_kwargs = {}

        if loop is None:
            loop = asyncio.get_event_loop()

        self.loop = loop

        self.amqp_url = amqp_url
        self.task = task
        self.queue_name = queue_name
        self._concurrency = concurrency
        self.tasks_per_worker = tasks_per_worker
        self.task_timeout = task_timeout
        self.reconnect_delay = reconnect_delay

        self.reject_exceptions = reject_exceptions + (Reject,)
        self.dead_letter_exceptions = dead_letter_exceptions + (DeadLetter,)

        self.queue_kwargs = queue_kwargs
        self.amqp_kwargs = amqp_kwargs

        self._queue = asyncio.Queue(loop=self.loop)
        self._workers = set()
        self._scale_lock = asyncio.Lock(loop=self.loop)
        self._stop_lock = asyncio.Lock(loop=self.loop)
        self._consume_callback_fut = self.loop.create_future()
        self._consume_callback_fut.set_result(None)
        self._down = asyncio.Event(loop=self.loop)
        self._down.set()
        self._up = asyncio.Event(loop=self.loop)

        self.__monitor = asyncio.ensure_future(self._monitor(), loop=self.loop)

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

    def _consume_callback(self, channel, payload, envelope, properties):
        task = payload, envelope, properties

        self._queue.put_nowait(task)

        return self._consume_callback_fut

    def _add_worker(self):
        worker = asyncio.ensure_future(self._worker(), loop=self.loop)

        self._workers.add(worker)

        worker.add_done_callback(self._workers.remove)

        msg = 'Added worker for queue: %(queue)s. ' \
              'Workers number is %(workers)s'
        context = {
            'queue': self.queue_name,
            'workers': len(self._workers)
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
        except aioamqp.AioamqpException as exc:
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
                _task = self.task(payload, properties)
            except self.reject_exceptions as exc:
                raise Reject from exc
            except self.dead_letter_exceptions as exc:
                raise DeadLetter from exc

            if not asyncio.iscoroutinefunction(unpartial(self.task)):
                if self.task_timeout is not None:
                    raise NotImplementedError

                raise Ack

            try:
                async with async_timeout.timeout(
                    self.task_timeout,
                    loop=self.loop,
                ) as cm:
                    try:
                        await _task
                    except self.reject_exceptions as exc:
                        raise Reject from exc
                    except self.dead_letter_exceptions as exc:
                        raise DeadLetter from exc
                    except asyncio.TimeoutError:
                        raise
            except asyncio.TimeoutError as exc:
                if cm.expired:
                    raise

                self._log_task('timeouted', logging.WARNING)
                return self._basic_reject(delivery_tag, requeue=True)
        except Ack:
            pass
        except DeadLetter:
            self._log_task('dead lettered')
            return self._basic_reject(delivery_tag, requeue=False)
        except Reject:
            self._log_task('rejected')
            return self._basic_reject(delivery_tag, requeue=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log_task('errored', lvl=logging.ERROR, exc_info=exc)
            return self._basic_client_ack(delivery_tag)

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

                    await asyncio.shield(finalizer, loop=self.loop)
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

    def _on_error_callback(self, exc):
        super()._on_error_callback(exc)
        if not self._down.is_set():
            self._down.set()

        if self._up.is_set():
            self._up.clear()

    async def ok(self, timeout=None):
        try:
            async with async_timeout.timeout(timeout, loop=self.loop):
                await self._up.wait()
        except asyncio.TimeoutError:
            raise aioamqp.AioamqpException

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
                except aioamqp.AioamqpException as exc:
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

    async def join(self, delay=1, timeout=None, wait_ok=True):
        queue_kwargs = self.queue_kwargs.copy()
        queue_kwargs.setdefault('passive', True)

        async with async_timeout.timeout(timeout, loop=self.loop):
            while True:
                if wait_ok:
                    await self.ok()

                await self._queue.join()

                try:
                    await asyncio.sleep(delay, loop=self.loop)

                    queue = await self._queue_declare(
                        queue_name=self.queue_name,
                        **queue_kwargs
                    )
                except aioamqp.AioamqpException as exc:
                    msg = 'Connection error during join in consumer ' \
                          '(queue: %(queue)s).'
                    context = {'queue': self.queue_name}
                    logger.warning(msg, context, exc_info=exc)
                    continue
                else:
                    if queue['message_count'] == 0 and self._queue.empty():
                        break

    async def _connect(self):
        while True:
            try:
                await super()._connect(self.amqp_url, **self.amqp_kwargs)

                await self._queue_declare(
                    queue_name=self.queue_name,
                    **self.queue_kwargs
                )

                self._up.set()

                msg = 'Consumer (queue: %(queue)s) connected to amqp'
                context = {'queue': self.queue_name}
                logger.debug(msg, context)

                break
            except aioamqp.AioamqpException as exc:
                msg = 'Cannot connect to amqp. Reconnect in %(delay)s seconds'
                context = {'delay': self.reconnect_delay}
                logger.warning(msg, context, exc_info=exc)

                await asyncio.sleep(self.reconnect_delay, loop=self.loop)

    async def _consume(self):
        while True:
            await self._connect()

            try:
                await self.scale(self._concurrency, wait_ok=False)
            except aioamqp.AioamqpException:
                continue

            try:
                consumer = await self._basic_consume(
                    self._consume_callback,
                    self.queue_name,
                    no_ack=False,
                )
            except aioamqp.AioamqpException:
                continue
            else:
                break

        self._consumer_tag = consumer['consumer_tag']

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
                    await asyncio.gather(*self._workers, loop=self.loop)

                msg = 'All workers (queue: %(queue)s) are stopped'
                context = {'queue': self.queue_name}
                logger.debug(msg, context)

                if flush_queue:
                    while self._queue.qsize():
                        self._queue.get_nowait()
                        self._queue.task_done()

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
                    except aioamqp.AioamqpException:
                        pass

                    msg = 'Consumer (queue: %(queue)s) stopped consuming'
                    context = {'queue': self.queue_name}
                    logger.debug(msg, context)

        self._consumer_tag = None

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

    async def __aenter__(self):  # noqa
        return self

    async def __aexit__(self, *exc_info):  # noqa
        self.close()
        await self.wait_closed()
