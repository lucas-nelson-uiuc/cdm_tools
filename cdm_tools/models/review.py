import itertools


def cdm_review(
    preparer: dict = None,
    senior_reviewer: dict = None,
    manager_reviewer: dict = None
) -> callable:
    def decorator(func: callable) -> callable:
        all_staff = tuple(
            staff
            for staff in (preparer, senior_reviewer, manager_reviewer)
            if staff is not None
        )
        assert (
            all_staff != tuple()
        ), ">>> [ERROR] No staff provided. Please provide at least two of preparer, senior_reviewer, or manager_reviewer."
        assert (
            len(all_staff) >= 2
        ), ">>> [ERROR] Not enough staff provided. Please provide at least two of preparer, senior_reviewer, or manager_reviewer."
        missing_signoffs = list(
            itertools.compress(
                all_staff, (not person.get("signoff") for person in all_staff)
            )
        )
        func._is_reviewed = True
        if missing_signoffs:
            print(
                f">>> [WARNING] Missing signoffs from: {', '.join(staff.get('name') for staff in missing_signoffs)}"
            )
            func._is_reviewed = False
        return func

    return decorator
