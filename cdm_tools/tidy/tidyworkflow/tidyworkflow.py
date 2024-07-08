from typing import Callable
import functools
from loguru import logger


def tidyworkflow(
    message: str = "Performing the following steps",
    validators: list[Callable] = None,
    section: str = None,
):
    """

    Examples
    ========
    >>> @tidyworkflow(
        message="Normalizes a string using the following steps:",
        validators=[lambda x: isinstance(x, str), lambda x: x == '@x@mpl@ str@ng']
    )
    def normalize_string_format(string, **kwargs):
        def remove_whitespace(string, **kwargs):
            '''Select/filter raw data to get working version of dataset'''
            return string.strip()

        def replace_vowels(string, **kwargs):
            '''Replace vowels with specific character (default '@')'''
            import re
            replacement = kwargs.get('replacement', '@')
            return re.sub('[aeiou]', repl=replacement, string=string, flags=re.IGNORECASE)

        def convert_to_case(string, **kwargs):
            '''Convert case using string method (default "lower")'''
            case_func = kwargs.get('case', 'lower')
            return getattr(str, case_func)(string)

        return locals()

    >>> normalize_string_format(string='EXamPle String')
    >>> normalize_string_format(string='EXamPle String', case='lower')
    >>> normalize_string_format(string='EXamPle String', case='lower', replacement='@')
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"#> ({func.__name__}) {message}")

            ### store, test local variables from function
            _locals = func(*args, **kwargs)
            if not isinstance(_locals, dict):
                raise ValueError(
                    f"{func.__name__} must return `locals()`, a dictionary"
                )

            ### separate local variables into appropriate groups
            _locals, _kwargs, _callables = bucket_local_vars(_locals=_locals)

            ### output "instructions" to user in console
            for i, _c in enumerate(_callables.values()):
                log_func = logger.info
                doc_string = _c.__doc__
                if getattr(_c, "_twf_ignore", False):
                    # TODO: remove callables from _callables composition
                    doc_string = f"Skipping {_c.__name__}"
                    log_func = logger.warning
                log_func(f"#>\t({str(i+1).rjust(2, '0')}) {doc_string}")

                section = kwargs.get("section")
                if not section in tidyworkflow.context:
                    tidyworkflow.context[section] = list()
                tidyworkflow.context[section].append(_c.__doc__)

            ### compose, return result of functions
            recipe = compose(*_callables.values())
            result = recipe(*_locals.values(), **_kwargs)

            ### (optional) validate result
            if validators is not None:
                assert all(map(lambda v: v(result), validators)), "Validators failed!"
            return result

        return wrapper

    return decorator


def tidyignore(func):
    """Decorator for ignoring functions in execution stack"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # TODO: pass (return None)
        return func(*args, **kwargs)

    # set attribute that can be checked, handled in tidyworkflow_memo
    wrapper._twf_ignore = True
    return wrapper


def compose(*functions):
    """Function composition with functools.reduce"""
    return functools.reduce(
        lambda f, g: lambda *args, **kwargs: f(g(*args, **kwargs), **kwargs), functions
    )


def bucket_local_vars(_locals: dict):
    """Separate callables from locals dictionary."""
    ### extract, remove keyword arguments from local scope
    _kwargs = _locals.get("kwargs")
    del _locals["kwargs"]

    ### extract, remove callables from local scope
    # TODO: consider removing ignore function from here to potentially
    # TODO: (continued) avoid attribute overhead in tidyworkflow_memo
    _callables = {k: v for k, v in _locals.items() if isinstance(v, Callable)}
    for callable in _callables:
        del _locals[callable]

    ### return groups of objects
    return _locals, _kwargs, _callables
