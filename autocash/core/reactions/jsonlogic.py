"""A small, safe JSONLogic interpreter for rule conditions (FT-8.5; docs/decisions.md D-3-1).

A condition is data, not code: {"and": [{"==": [{"var": "type"}, "order.created"]},
{">": [{"var": "fields.amount"}, 1000]}]}. Only the operators below exist; there is no access
to Python objects, attributes or functions, and evaluation is bounded by the size of the rule.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

MAX_DEPTH = 32


class LogicError(ValueError):
    pass


def _get(data: Any, path: str, default: Any = None) -> Any:
    if path == "":
        return data
    cur = data
    for part in str(path).split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return default
            cur = cur[part]
        elif isinstance(cur, (list, tuple)) and part.lstrip("-").isdigit():
            idx = int(part)
            if not -len(cur) <= idx < len(cur):
                return default
            cur = cur[idx]
        else:
            return default
    return cur


def _num(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return float("nan")


def truthy(value: Any) -> bool:
    if isinstance(value, (list, tuple, dict, str)):
        return len(value) > 0
    return bool(value) and value != 0


def _cmp(op: Callable[[Any, Any], bool]) -> Callable[[list[Any]], bool]:
    def run(args: list[Any]) -> bool:
        a, b = args[0], args[1]
        if not (isinstance(a, str) and isinstance(b, str)):
            a, b = _num(a), _num(b)
        try:
            if len(args) == 3:  # between: {"<": [1, x, 10]}
                c = args[2] if isinstance(args[2], str) and isinstance(args[1], str) else _num(args[2])
                return op(a, b) and op(b, c)
            return op(a, b)
        except TypeError:
            return False
    return run


_SIMPLE: dict[str, Callable[[list[Any]], Any]] = {
    "==": lambda a: a[0] == a[1] or (str(a[0]) == str(a[1]) if a[0] is not None and a[1] is not None else False),
    "!=": lambda a: not (a[0] == a[1] or (str(a[0]) == str(a[1]) if a[0] is not None and a[1] is not None else False)),
    ">": _cmp(lambda x, y: x > y),
    ">=": _cmp(lambda x, y: x >= y),
    "<": _cmp(lambda x, y: x < y),
    "<=": _cmp(lambda x, y: x <= y),
    "!": lambda a: not truthy(a[0]),
    "!!": lambda a: truthy(a[0]),
    "in": lambda a: a[1] is not None and (str(a[0]) in a[1] if isinstance(a[1], str) else a[0] in a[1]),
    "cat": lambda a: "".join("" if x is None else str(x) for x in a),
    "+": lambda a: sum(_num(x) for x in a),
    "-": lambda a: -_num(a[0]) if len(a) == 1 else _num(a[0]) - _num(a[1]),
    "*": lambda a: _num(a[0]) * _num(a[1]),
    "/": lambda a: _num(a[0]) / _num(a[1]) if _num(a[1]) else float("nan"),
    "lower": lambda a: str(a[0] or "").lower(),
    "startswith": lambda a: str(a[0] or "").startswith(str(a[1])),
    "count": lambda a: len(a[0]) if isinstance(a[0], (list, tuple, dict, str)) else 0,
}


def apply(logic: Any, data: Any, _depth: int = 0) -> Any:
    if _depth > MAX_DEPTH:
        raise LogicError("condition is nested too deeply")
    if isinstance(logic, list):
        return [apply(x, data, _depth + 1) for x in logic]
    if not isinstance(logic, dict):
        return logic
    if len(logic) != 1:
        raise LogicError(f"an operation must have exactly one operator: {sorted(logic)}")
    op, raw = next(iter(logic.items()))
    args = raw if isinstance(raw, list) else [raw]
    if op == "var":
        path = apply(args[0], data, _depth + 1) if args else ""
        default = apply(args[1], data, _depth + 1) if len(args) > 1 else None
        return _get(data, "" if path is None else str(path), default)
    if op == "missing":
        names = apply(args, data, _depth + 1)
        return [n for n in names if _get(data, str(n)) in (None, "")]
    if op == "and":
        value: Any = True
        for x in args:
            value = apply(x, data, _depth + 1)
            if not truthy(value):
                return value
        return value
    if op == "or":
        value = False
        for x in args:
            value = apply(x, data, _depth + 1)
            if truthy(value):
                return value
        return value
    if op in ("if", "?:"):
        i = 0
        while i + 1 < len(args):
            if truthy(apply(args[i], data, _depth + 1)):
                return apply(args[i + 1], data, _depth + 1)
            i += 2
        return apply(args[i], data, _depth + 1) if i < len(args) else None
    fn = _SIMPLE.get(op)
    if fn is None:
        raise LogicError(f"unknown operator {op!r}")
    values = [apply(x, data, _depth + 1) for x in args]
    if op in ("==", "!=", ">", ">=", "<", "<=", "in", "*", "/", "startswith") and len(values) < 2:
        raise LogicError(f"operator {op!r} needs two arguments")
    return fn(values)


def validate(logic: Any, _depth: int = 0) -> None:
    """Static check at config load: known operators only, sane nesting (FT-13.2)."""
    if _depth > MAX_DEPTH:
        raise LogicError("condition is nested too deeply")
    if isinstance(logic, list):
        for x in logic:
            validate(x, _depth + 1)
        return
    if not isinstance(logic, dict):
        return
    if len(logic) != 1:
        raise LogicError(f"an operation must have exactly one operator: {sorted(logic)}")
    op, raw = next(iter(logic.items()))
    if op not in _SIMPLE and op not in ("var", "missing", "and", "or", "if", "?:"):
        raise LogicError(f"unknown operator {op!r}")
    validate(raw, _depth + 1)
