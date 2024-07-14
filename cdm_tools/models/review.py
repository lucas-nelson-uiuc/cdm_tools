import itertools


def cdm_review(**kwargs) -> callable:
    """
    Decorator for making a function 'reviewable', allowing staff to approve or
    disprove procedures performed. Additionally, tracks staff involved in an
    engagement to be used in memos (if necessary).
    """

    if not hasattr(cdm_review, "_staff"):
        setattr(cdm_review, "_staff", dict())

    def decorator(func: callable) -> callable:
        assert (
            kwargs != dict()
        ), ">>> [ERROR] No staff provided. Please provide at least two of personnel."
        assert (
            len(kwargs.keys()) >= 2
        ), ">>> [ERROR] Not enough staff provided. Please provide at least two personnel."

        # reformat staff in dictionary structure
        staff = {
            staff.get("name"): {
                "title": staff.get("title", "Missing Title"),
                "signoff": staff.get("signoff", False),
            }
            for _, staff in kwargs.items()
        }

        # udpate staff listing, check if signoffs are missing
        cdm_review._staff |= staff
        missing_signoffs = list(
            itertools.compress(
                cdm_review._staff,
                (not person.get("signoff") for person in cdm_review._staff),
            )
        )

        # switch review status based on signoffs
        func._is_reviewed = True
        if missing_signoffs:
            # print(
            #     f">>> [WARNING] Missing signoffs from: {', '.join(staff.get('name') for staff in missing_signoffs)}"
            # )
            func._is_reviewed = False
        return func

    return decorator
