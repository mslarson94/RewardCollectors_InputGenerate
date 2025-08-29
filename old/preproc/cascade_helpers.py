def generate_synthetic_events(base_time, timestamp_str, event_specs, common_info):
    result = []
    for spec in event_specs:
        event = dict(common_info)
        event.update({
            "Timestamp": base_time + spec.get("Offset", 0),
            "AppTime": timestamp_str,
            "Event": spec["Event"],
        })
        if "Role" in spec:
            event["Role"] = spec["Role"]
        if "Value" in spec:
            event["Value"] = spec["Value"]
        if "Details" in spec:
            event["Details"] = spec["Details"]
        result.append(event)
    return result
