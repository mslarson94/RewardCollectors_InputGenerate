#!/usr/bin/env python3
# randomize_roles.py

from __future__ import annotations

import argparse
import random
from collections.abc import Sequence
from typing import TypeVar


ItemType = TypeVar("ItemType")
RoleType = TypeVar("RoleType")


def assign_roles_without_replacement(
    items: Sequence[ItemType],
    roles: Sequence[RoleType],
    seed: int | None = None,
) -> dict[ItemType, RoleType]:
    """Randomly assign one unique role to each item.

    Args:
        items: Items that need role assignments.
        roles: Available roles. There must be at least as many roles as items.
        seed: Optional seed for reproducible assignments.

    Returns:
        A dictionary mapping each item to one unique role.

    Raises:
        ValueError: If items or roles are empty, contain duplicates, or there
            are fewer roles than items.
        TypeError: If an item cannot be used as a dictionary key.
    """
    item_list = list(items)
    role_list = list(roles)

    if not item_list:
        raise ValueError("At least one item is required.")

    if not role_list:
        raise ValueError("At least one role is required.")

    try:
        unique_items = set(item_list)
    except TypeError as exc:
        raise TypeError(
            "Every item must be hashable so it can be used as a dictionary key."
        ) from exc

    if len(unique_items) != len(item_list):
        raise ValueError("Items must be unique.")

    try:
        unique_roles = set(role_list)
    except TypeError as exc:
        raise TypeError("Every role must be hashable.") from exc

    if len(unique_roles) != len(role_list):
        raise ValueError("Roles must be unique.")

    if len(role_list) < len(item_list):
        raise ValueError(
            f"Not enough roles: received {len(role_list)} roles "
            f"for {len(item_list)} items."
        )

    random_generator = random.Random(seed)
    shuffled_roles = role_list.copy()
    random_generator.shuffle(shuffled_roles)

    return dict(zip(item_list, shuffled_roles, strict=False))


def main() -> None:
    """Run an example random assignment."""
    parser = argparse.ArgumentParser(
        description="Assign unique roles to items without replacement."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible assignments.",
    )
    args = parser.parse_args()

    ## Excluding the C & D coin sets 
    mainCohort = [
        "09_A_main",
        "00_Ax_main",
        "02_Ax_main",
        "200_B_main",
        "02_Bx_main",
        "05_A_main",
        "06_A_main",
        "07_A_main",
        "08_A_main",
        "200_A_main",
        "03_Ax_main",
        "99_Ax_main",
        "05_B_main",
        "06_B_main",
        "07_B_main",
        "08_B_main",
        "01_Bx_main",
        "03_Bx_main",
    ]
    ## Excluding the C & D coin sets 
    rrCohort = [
        "05_A_RR",
        "06_A_RR",
        "07_A_RR",
        "08_A_RR",
        "00_Ax_RR",
        "06_B_RR",
        "07_B_RR",
        "02_Bx_RR",
    ]

    main_roles = [
        "single representative",
        "pin drop distance",
        "round elapsed time",
        "pathChoice",
        "path efficiency",
        "swap vote score",
        "pin drop vote score",
        "total score",
        "swap rate",
        "path example",
        "distance-points trade off",
    ]


    rr_roles1 = [
        "single representative",
        "pin drop distance",
        "round elapsed time",
        "pathChoice",
        "path efficiency",
        "swap vote score",
        "pin drop vote score",
        "total score",
    ]

    rr_roles2 = [
        "swap rate",
        "path example",
        "distance-points trade off",
    ]

    main_assignments = assign_roles_without_replacement(
        items=main_roles,
        roles=mainCohort,
        seed=args.seed,
    )

    for item, role in main_assignments.items():
        print(f"{item}: {role}")

    print('/n'*5)

    rr_assignments1 = assign_roles_without_replacement(
        items=rr_roles1,
        roles=rrCohort,
        seed=args.seed,
    )

    rr_assignments2 = assign_roles_without_replacement(
        items=rr_roles2,
        roles=rrCohort,
        seed=args.seed,
    )

    for item, role in rr_assignments1.items():
        print(f"{item}: {role}")
    for item, role in rr_assignments2.items():
        print(f"{item}: {role}")




if __name__ == "__main__":
    main()