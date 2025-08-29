# 🧪 To validate a real cascade against this template, ask ChatGPT: "How can I check if events from my ideal test file match the canonical pinDrop or chestOpen structure?"

from cascade_templates import CANONICAL_CASCADE_TEMPLATES

def get_expected_event_sequence(group_type):
    """
    Returns the list of expected event types (with tags) for a given canonical group_type.
    """
    if group_type not in CANONICAL_CASCADE_TEMPLATES:
        raise ValueError(f"Unknown group_type: {group_type}")

    return [
        (step["type"], step.get("tag", ""), step.get("optional", False))
        for step in CANONICAL_CASCADE_TEMPLATES[group_type]["sequence"]
    ]

def print_example_structure():
    for group in CANONICAL_CASCADE_TEMPLATES:
        print(f"Group Type: {group}")
        for step in CANONICAL_CASCADE_TEMPLATES[group]["sequence"]:
            tag = step.get("tag", "")
            optional = " (optional)" if step.get("optional", False) else ""
            print(f"  - {step['type']}{' → ' + tag if tag else ''}{optional}")
        print()

if __name__ == "__main__":
    print_example_structure()
