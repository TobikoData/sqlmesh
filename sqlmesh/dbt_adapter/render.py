import typing as t

from jinja2 import Environment, Undefined


class Placeholder(str):
    def __call__(self, *args, **kwargs):
        return ""


class UndefinedSpy(Undefined):
    def _fail_with_undefined_error(self, *args, **kwargs):
        return Placeholder()

    __add__ = __radd__ = __sub__ = __rsub__ = _fail_with_undefined_error
    __mul__ = __rmul__ = __div__ = __rdiv__ = _fail_with_undefined_error
    __truediv__ = __rtruediv__ = _fail_with_undefined_error
    __floordiv__ = __rfloordiv__ = _fail_with_undefined_error
    __mod__ = __rmod__ = _fail_with_undefined_error
    __pos__ = __neg__ = _fail_with_undefined_error
    __call__ = __getitem__ = _fail_with_undefined_error
    __lt__ = __le__ = __gt__ = __ge__ = _fail_with_undefined_error
    __int__ = __float__ = __complex__ = _fail_with_undefined_error
    __pow__ = __rpow__ = _fail_with_undefined_error


def render_jinja(query: str, env: t.Optional[t.Dict[str, t.Callable]] = None) -> str:
    """
    Render the jinja in the provided string using the passed in environment

    Args:
        query: The string to render
        env: jinja methods to use during rendering

    Returns:
        The jinja rendered string
    """
    env = env or {}

    jinja_env = Environment(undefined=UndefinedSpy)

    template = jinja_env.from_string(query)
    return template.render(env)
