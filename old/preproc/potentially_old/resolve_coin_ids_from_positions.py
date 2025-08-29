
def resolve_coin_ids_from_positions(events, coinset_dict, position_threshold=0.15):
    def find_matching_coin_label(x, z):
        for label, props in coinset_dict.items():
            cx, cz = props["coords"]
            if abs(cx - x) <= position_threshold and abs(cz - z) <= position_threshold:
                return label
        return None

    for e in events:
        if e.get("event_type") == "PinDrop":
            details = e.get("details", {})
            x = details.get("pin_local_x")
            z = details.get("pin_local_z")
            if x is not None and z is not None:
                match = find_matching_coin_label(round(x, 1), round(z, 1))
                if match:
                    e["details"]["matchedCoinLabel"] = match
    return events
