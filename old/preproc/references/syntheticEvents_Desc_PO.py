#### Pin Drop Event 
## Example Trigger Message: "Other participant just dropped a new pin at -1.531 -1.557 -8.773"
if row["Type"] == "Event" and isinstance(row["Message"], str) and "Other participant just dropped a new pin at " in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            events.append({
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "AN just dropped a pin",
                "details": {},
                "source": "logged"
            })

            ...

            synthetic_events = [
                            (0.000, "Pin drop sound (start)"),
                            (0.000, "Voting Window (start)")
                            (0.000, "Gray pin visible (start)"),
                            (0.654, "Pin drop sound (end)"),
                            (2.000, "Voting Window (end)")
                            (2.000, "Gray pin visible (end)"),
                            (2.000, "Feedback sound (start)"),
                            (2.000, "Feedback text and color visible (start)"),
                            (2.000, "Coin value text visible (start)")
                            (2.650, "Feedback sound (end)"),
                            (3.000, "Feedback text and color visible (end)"),
                            (3.000, "Coin visible (start)"),
                            (3.000, "Coin presentation sound (start)"),
                            (3.650, "Coin presentation sound (end)")
                        ]

#### Feedback Coin Collect 
## Example Trigger Message: "A.N. collected coin:0 round reward: 0.00"
if row["Type"] == "Event" and isinstance(row["Message"], str) and "A.N. collected coin:" in row["Message"]: 
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            events.append({
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "AN collected pin feedback coin",
                "details": {},
                "source": "logged"
            })

            ...

            synthetic_events = [
                            (0.000, "Coin is visible (end)"),
                            (0.000, "Coin collection sound (start)"),
                            (0.654, "Coin collection sound (end)"),
                            (2.000, "Coin value text visible (end)"),
                        ]

#### IE Coin Collect 
## Example Trigger Message: "Other participant just collected coin: 0"
if row["Type"] == "Event" and isinstance(row["Message"], str) and "coin collected:" in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            events.append({
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "Collected IE coin",
                "details": {},
                "source": "logged"
            })

            ...

            synthetic_events = [
                            (0.000, "Coin is visible (end)"),
                            (0.000, "Coin collection sound (start)"),
                            (0.654, "Coin collection sound (end)"),
                            (2.000, "Coin value text visible (end)"),
                        ]