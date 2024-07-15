from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, wait
from functools import wraps
from typing import TYPE_CHECKING, Any, Protocol

import greenlet
from greenlet import getcurrent
from typing_extensions import ParamSpec, TypeVar

from jdbc_wrapper import exceptions
from jdbc_wrapper.utils_async import await_ as jdbc_await
from jdbc_wrapper.utils_async import check_async_greenlet, check_in_sa_greenlet

_T_co = TypeVar("_T_co", covariant=True)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    class AwaitFunc(Protocol):
        def __call__(
            self, awaitable: Awaitable[_T_co] | Coroutine[Any, Any, _T_co]
        ) -> _T_co: ...


__all__ = []

_T = TypeVar("_T")
_P = ParamSpec("_P")


def wrap_async(
    func: Callable[_P, Awaitable[_T]] | Callable[_P, Coroutine[Any, Any, _T]],
) -> Callable[_P, _T]:
    @wraps(func)
    def inner(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        current = greenlet.getcurrent()
        if not check_async_greenlet(current) and not check_in_sa_greenlet(current):
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(_run, func, *args, **kwargs)
                wait([future], return_when="ALL_COMPLETED")
                return future.result()

        coro = func(*args, **kwargs)
        return jdbc_await(coro)

    return inner


def await_fallback(awaitable: Awaitable[_T_co] | Coroutine[Any, Any, _T_co]) -> _T_co:
    current = getcurrent()
    if not check_async_greenlet(current):
        if check_in_sa_greenlet(current):
            from sqlalchemy.util import await_fallback as sa_await_fallback

            return sa_await_fallback(awaitable)

        with ThreadPoolExecutor(1) as pool:
            coro = _awaitable_to_coro(awaitable)
            future = pool.submit(asyncio.run, coro)
            wait([future], return_when="ALL_COMPLETED")
            return future.result()

        raise exceptions.OperationalError("greenlet_spawn has not been called")
    return current.parent.switch(awaitable)


def await_only(awaitable: Awaitable[_T_co] | Coroutine[Any, Any, _T_co]) -> _T_co:
    current = getcurrent()
    if not check_async_greenlet(current):
        if check_in_sa_greenlet(current):
            from sqlalchemy.util import await_only as sa_await_only

            return sa_await_only(awaitable)

        with ThreadPoolExecutor(1) as pool:
            coro = _awaitable_to_coro(awaitable)
            future = pool.submit(asyncio.run, coro)
            wait([future], return_when="ALL_COMPLETED")
            return future.result()

        raise exceptions.OperationalError("greenlet_spawn has not been called")
    return current.parent.switch(awaitable)


async def _await(
    func: Callable[_P, Awaitable[_T]] | Callable[_P, Coroutine[Any, Any, _T]],
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    return await func(*args, **kwargs)


def _run(
    func: Callable[_P, Awaitable[_T]] | Callable[_P, Coroutine[Any, Any, _T]],
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    return asyncio.run(_await(func, *args, **kwargs))


def _awaitable_to_coro(
    awaitable: Awaitable[_T_co] | Coroutine[Any, Any, _T_co],
) -> Coroutine[Any, Any, _T_co]:
    async def coro() -> Any:
        return await awaitable

    return coro()
