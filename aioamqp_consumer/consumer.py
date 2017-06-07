import asyncio
import logging

from functools import partial

import aioamqp
import async_timeout

from .compat import create_future, create_task, iscoroutine, PY_350
from .exceptions import Ack, DeadLetter, Reject
from .log import logger
from .mixins import AMQPMixin


class Consumer(AMQPMixin):

    _consumer_tag = None

    def __init__(
        self,
        amqp_url,
        task,
        queue_name,
        concurrency=1,
        tasks_per_worker=3,
        task_timeout=None,
        reconnect_delay=5.0,
        reject_exceptions=tuple(),
        dead_letter_exceptions=tuple(),
        *,
        loop=None,
        **amqp_kwargs
    ):
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

        self.amqp_kwargs = amqp_kwargs

        self._queue = asyncio.Queue(loop=self.loop)
        self._workers = set()
        self._scale_lock = asyncio.Lock(loop=self.loop)
        self._stop_lock = asyncio.Lock(loop=self.loop)
        self._consume_callback_fut = create_future(loop=self.loop)
        self._consume_callback_fut.set_result(None)
        self._down = asyncio.Event(loop=self.loop)
        self._down.set()
        self._up = asyncio.Event(loop=self.loop)

        self.__monitor = create_task(loop=self.loop)(self._monitor())

    @asyncio.coroutine
    def _get_concurrency(self):
        with (yield from self._scale_lock):
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
        worker = create_task(loop=self.loop)(self._worker())

        self._workers.add(worker)

        worker.add_done_callback(self._workers.remove)

        logger.debug(
            'Added worker for queue: {queue}. Workers number is {workers}'
            .format(
                queue=self.queue_name,
                workers=len(self._workers),
            ),
        )

    @asyncio.coroutine
    def _remove_worker(self):
        worker = next(iter(self._workers))

        worker.cancel()

        try:
            yield from worker
        except aioamqp.AioamqpException as exc:
            logger.debug(
                'Worker (queue: {queue}) faced connection problems'.format(
                    queue=self.queue_name,
                ),
                exc_info=exc,
            )

        logger.debug(
            'Removed worker for queue: {queue}. Workers number is {workers}'
            .format(
                queue=self.queue_name,
                workers=len(self._workers),
            ),
        )

    def _log_task(
        self,
        status,
        delivery_tag,
        lvl=logging.DEBUG,
    ):
        logger.log(
            lvl,
            'Task (queue: {queue},  tag: {delivery_tag}) {status}'  # noqa
            .format(
                queue=self.queue_name,
                delivery_tag=delivery_tag,
                status=status,
            ),
        )

    @asyncio.coroutine
    def _run_task(self, payload, properties, delivery_tag):
        log = partial(
            self._log_task,
            delivery_tag=delivery_tag,
        )

        try:
            try:
                _task = self.task(payload, properties)
            except self.reject_exceptions as exc:
                raise Reject from exc
            except self.dead_letter_exceptions as exc:
                raise DeadLetter from exc

            if not iscoroutine(_task):
                if self.task_timeout is not None:
                    raise NotImplementedError

                raise Ack

            task_timeout = False

            try:
                with async_timeout.timeout(self.task_timeout, loop=self.loop):
                    try:
                        yield from _task
                    except self.reject_exceptions as exc:
                        raise Reject from exc
                    except self.dead_letter_exceptions as exc:
                        raise DeadLetter from exc
                    except asyncio.TimeoutError:
                        task_timeout = True
                        raise
            except asyncio.TimeoutError as exc:
                if task_timeout:
                    raise

                log('timeouted', logging.WARNING)
                return self._basic_reject(delivery_tag, requeue=True)
        except Ack:
            pass
        except DeadLetter:
            log('dead lettered')
            return self._basic_reject(delivery_tag, requeue=False)
        except Reject:
            log('rejected')
            return self._basic_reject(delivery_tag, requeue=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log('errored', lvl=logging.WARNING)
            logger.warning(exc, exc_info=exc)
            return self._basic_client_ack(delivery_tag)

        log(status='successfully finished')
        return self._basic_client_ack(delivery_tag)

    @asyncio.coroutine
    def _worker(self):
        while True:
            delivery_tag, finalizer = None, None
            try:
                payload, envelope, properties = yield from self._queue.get()
                delivery_tag = envelope.delivery_tag

                try:
                    finalizer = yield from self._run_task(
                        payload,
                        properties,
                        delivery_tag,
                    )

                    yield from finalizer
                finally:
                    self._queue.task_done()
            except asyncio.CancelledError:
                if (
                    delivery_tag is not None and
                    finalizer is None and
                    not self._down.is_set()
                ):
                    yield from self._basic_reject(delivery_tag, requeue=True)

                logger.debug('Worker (queue: {queue}) is cancelled'.format(
                    queue=self.queue_name,
                ))

                break

    def _on_error_callback(self, exc):
        super()._on_error_callback(exc)
        if not self._down.is_set():
            self._down.set()

        if self._up.is_set():
            self._up.clear()

    @asyncio.coroutine
    def ok(self, timeout=None):
        try:
            with async_timeout.timeout(timeout, loop=self.loop):
                yield from self._up.wait()
        except asyncio.TimeoutError:
            raise aioamqp.AioamqpException

    @asyncio.coroutine
    def scale(self, concurrency, wait_ok=True, timeout=None):
        if concurrency <= 0:
            raise NotImplementedError

        assert not self._closed

        if wait_ok:
            yield from self.ok(timeout=timeout)

        with (yield from self._scale_lock):
            # for initial scale, compare to real amount of workers
            current_concurrency = len(self._workers)

            logger.debug(
                'Scaling workers from {prev_conc} to {next_conc}'.format(
                    prev_conc=current_concurrency,
                    next_conc=concurrency,
                ),
            )

            self._concurrency = concurrency

            if self._concurrency != current_concurrency:
                prefetch = self._concurrency * self.tasks_per_worker
                try:
                    yield from self._basic_qos(
                        prefetch_size=0,
                        prefetch_count=prefetch,
                        connection_global=True,
                    )
                except aioamqp.AioamqpException as exc:
                    logger.warning(
                        'Connection problem during consumer scale '
                        '(queue: {queue}). Not scaled. Scale will be performed'
                        ' by monitor on next iteration'.format(
                            queue=self.queue_name,
                        ),
                    )
                    return

            if current_concurrency > self._concurrency:
                for _ in range(current_concurrency - self._concurrency):
                    yield from self._remove_worker()
            elif current_concurrency < self._concurrency:
                for _ in range(self._concurrency - current_concurrency):
                    self._add_worker()

            assert len(self._workers) == self._concurrency

        logger.debug('Scaled workers from {prev_conc} to {curr_conc}'.format(
            prev_conc=current_concurrency,
            curr_conc=self._concurrency,
        ))

    @asyncio.coroutine
    def join(self, delay=1, timeout=None, wait_ok=True, **queue_kwargs):
        queue_kwargs.setdefault('passive', True)

        with async_timeout.timeout(timeout, loop=self.loop):
            while True:
                if wait_ok:
                    yield from self.ok()

                yield from self._queue.join()

                try:
                    yield from asyncio.sleep(delay, loop=self.loop)

                    queue = yield from self._queue_declare(
                        queue_name=self.queue_name,
                        **queue_kwargs
                    )
                except aioamqp.AioamqpException as exc:
                    logger.warning(
                        'Connection error during join in consumer '
                        '(queue: {queue}).'.format(
                            queue=self.queue_name,
                        ),
                    )
                    continue
                else:
                    if queue['message_count'] == 0 and self._queue.empty():
                        break

    @asyncio.coroutine
    def _connect(self):
        while True:
            try:
                yield from super()._connect(self.amqp_url, **self.amqp_kwargs)

                yield from self._queue_declare(
                    queue_name=self.queue_name,
                    durable=True,
                )

                self._up.set()

                logger.debug(
                    'Consumer (queue: {queue}) connected to amqp'.format(
                        queue=self.queue_name,
                    ),
                )

                break
            except aioamqp.AioamqpException as exc:
                logger.warning(
                    'Cannot connect to amqp. Reconnect in {delay} seconds'
                    .format(
                        delay=self.reconnect_delay,
                    ),
                    exc_info=True,
                )

                yield from asyncio.sleep(self.reconnect_delay, loop=self.loop)

    @asyncio.coroutine
    def _consume(self):
        while True:
            yield from self._connect()

            try:
                yield from self.scale(self._concurrency, wait_ok=False)
            except aioamqp.AioamqpException:
                continue

            try:
                consumer = yield from self._basic_consume(
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

        logger.debug('Consumer (queue: {queue}) started consuming'.format(
            queue=self.queue_name,
        ))

    @asyncio.coroutine
    def _stop(self, cancel_workers=True, flush_queue=True):
        with (yield from self._stop_lock):
            if self._up.is_set():
                self._up.clear()

            logger.debug('Consumer (queue: {queue}) is stopped'.format(
                queue=self.queue_name,
            ))

            with (yield from self._scale_lock):
                if cancel_workers:
                    self.__cancel_workers()

                if self._workers:
                    yield from asyncio.gather(*self._workers, loop=self.loop)
                logger.debug('All workers (queue: {queue}) are stopped'.format(
                    queue=self.queue_name,
                ))

                if flush_queue:
                    while self._queue.qsize():
                        self._queue.get_nowait()
                        self._queue.task_done()

            yield from self._disconnect()

    def __cancel_workers(self):
        logger.debug('Stopping consumer workers (queue: {queue})'.format(
            queue=self.queue_name,
        ))

        for worker in self._workers:
            assert not worker.cancelled(), 'Stopped worker detected'

            worker.cancel()

    @asyncio.coroutine
    def _monitor(self):
        while True:
            try:
                yield from self._consume()

                yield from self._down.wait()

                yield from self._stop()
            except asyncio.CancelledError:
                break

    @asyncio.coroutine
    def _disconnect(self):
        if self._transport is not None and self._protocol is not None:
            if self._channel is not None:
                if self._consumer_tag is not None:
                    try:
                        _basic_cancel = self._channel.basic_cancel(
                            self._consumer_tag,
                        )

                        yield from asyncio.shield(_basic_cancel, loop=self.loop)
                    except aioamqp.AioamqpException:
                        pass

                    logger.debug(
                        'Consumer (queue: {queue}) stopped consuming'.format(
                            queue=self.queue_name,
                        ),
                    )

        self._consumer_tag = None

        yield from super()._disconnect()

        logger.debug(
            'Consumer (queue: {queue}) disconnected from amqp'.format(
                queue=self.queue_name,
            ))

    def close(self):
        assert not self._closed, 'Already closed'

        self._closed = True

        self.__monitor.cancel()
        self.__cancel_workers()

        logger.debug('Consumer (queue: {queue}) is closing'.format(
            queue=self.queue_name,
        ))

    @asyncio.coroutine
    def wait_closed(self):
        assert self._closed, 'Must be closed first'

        try:
            yield from self.__monitor
        except asyncio.CancelledError:
            pass

        yield from self._stop(cancel_workers=False)

        yield from self._consume_callback_fut

        logger.debug('Consumer (queue: {queue}) closed'.format(
            queue=self.queue_name,
        ))

    if PY_350:
        @asyncio.coroutine
        def __aenter__(self):  # noqa
            return self

        @asyncio.coroutine
        def __aexit__(self, *exc_info):  # noqa
            self.close()
            yield from self.wait_closed()
