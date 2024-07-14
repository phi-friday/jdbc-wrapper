from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from greenlet import getcurrent, greenlet
from typing_extensions import ParamSpec, TypeGuard, TypeVar

from jdbc_wrapper import exceptions

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from jdbc_wrapper.abc import AsyncConnectionABC, AsyncCursorABC

__all__ = []

_T_co = TypeVar("_T_co", covariant=True)
_P = ParamSpec("_P")


class AsyncGreenlet(greenlet):
    parent: greenlet  # pyright: ignore[reportIncompatibleVariableOverride]

    def __init__(self, run: Callable[..., Any], parent: greenlet) -> None:
        super().__init__(run, parent)
        self._async_greelnet_ = True
        self.gr_context = parent.gr_context


def check_in_greelnet() -> bool:
    current = getcurrent()
    return check_async_greenlet(current)


def check_async_greenlet(current: greenlet) -> TypeGuard[AsyncGreenlet]:
    return getattr(current, "_async_greelnet_", False)


def check_in_sa_greenlet(current: greenlet) -> bool:
    return getattr(current, "__sqlalchemy_greenlet_provider__", False)


def await_(awaitable: Awaitable[_T_co] | Coroutine[Any, Any, _T_co]) -> _T_co:
    current = getcurrent()
    if not check_async_greenlet(current):
        if check_in_sa_greenlet(current):
            from sqlalchemy.util import await_only

            return await_only(awaitable)
        raise exceptions.OperationalError("greenlet_spawn has not been called")
    return current.parent.switch(awaitable)


async def greenlet_spawn(
    func: Callable[_P, _T_co], *args: _P.args, **kwargs: _P.kwargs
) -> _T_co:
    current = getcurrent()
    context = AsyncGreenlet(func, current)
    result = context.switch(*args, **kwargs)
    while not context.dead:
        result = await greenlet_spawn_process(context, result)

    return result


async def greenlet_spawn_process(context: greenlet, result: Any) -> Any:
    try:
        value = await result
    except BaseException:  # noqa: BLE001
        exc_info = sys.exc_info()
        result = context.throw(*exc_info)  # type: ignore
    else:
        result = context.switch(value)
    return result


async def ensure_close(
    connection_or_cursor: AsyncConnectionABC[Any] | AsyncCursorABC[Any],
) -> None:
    if not connection_or_cursor.is_closed:
        await connection_or_cursor.close()
