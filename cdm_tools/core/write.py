def cp_write(
    *args,
    write_func: callable,
    preprocess_function: callable = None,
    **kwargs
) -> None:
    """Integrate DocuSign feature for file writing."""
    if preprocess_function is None:
        raise ValueError("Must pass function to `preprocess_function`")
    if not hasattr(preprocess_function, "_is_reviewed"):
        raise AssertionError("Passed function not decorated with `cdm_review`")
    if not preprocess_function._is_reviewed:
        raise PermissionError("Passed function does not have all required signoffs")
    print(f"Writing data to path: {args[1] if args else kwargs.get('path')}")
    write_func(*args, **kwargs)
    