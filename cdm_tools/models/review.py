import itertools


def cdm_review(preparer: dict, senior_reviewer: dict, manager_reviewer: dict):
    def decorator(func: callable):
        all_staff = (preparer, senior_reviewer, manager_reviewer)
        missing_signoffs = list(
            itertools.compress(
                all_staff, (not person.get("signoff") for person in all_staff)
            )
        )
        func._is_reviewed = True
        if missing_signoffs:
            print(
                f"Missing signoffs from: {', '.join(staff.get('name') for staff in missing_signoffs)}"
            )
            func._is_reviewed = False
        return func

    return decorator
