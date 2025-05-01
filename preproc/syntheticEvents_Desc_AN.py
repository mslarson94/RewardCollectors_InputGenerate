#### Pin Drop Event 
## Trigger Message is always "Just dropped a pin"
if row["Type"] == "Event" and isinstance(row["Message"], str) and "Just dropped a pin" in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            events.append({
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "Just dropped a pin",
                "details": {},
                "source": "logged"
            })

            ...

            synthetic_events = [
                            (0.000, "Pin drop sound (start)"),
                            (0.000, "Gray pin visible (start)"),
                            (0.654, "Pin drop sound (end)"),
                            (2.000, "Gray pin visible (end)"),
                            (2.000, "Feedback sound (start)"),
                            (2.000, "Feedback text and color visible (start)"),
                            (2.000, "Coin value text visible (start)")
                            (2.650, "Feedback sound end"),
                            (3.000, "Feedback text and color visible (end)"),
                            (3.000, "Coin visible (start)"),
                            (3.000, "Coin presentation sound (start)"),
                            (3.650, "Coin presentation sound (end)"),
                            (4.000, "Coin is released for collection.")
                        ]

#### Feedback Coin Collect 
## Example Trigger Message: "Collected pin feedback coin:0"
if row["Type"] == "Event" and isinstance(row["Message"], str) and "Collected pin feedback coin:" in row["Message"]: 
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            events.append({
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "Collected pin feedback coin",
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


#### IE Chest Open
## Example Trigger Message: "Chest opened:1"
if row["Type"] == "Event" and isinstance(row["Message"], str) and "Chest opened:" in row["Message"]:
            cascade_id += 1
            start_time = row["AppTime"]
            timestamp = row["Timestamp"]
            events.append({
                "AppTime": start_time,
                "Timestamp": timestamp,
                "cascade_id": cascade_id,
                "event_type": "Chest opened",
                "details": {},
                "source": "logged"
            })

            ...

            synthetic_events = [
                            (0.000, "Chest opening (start)"),
                            (0.000, "Chest opening sound (start)"),
                            (0.400, "Chest opening (end)")
                            (0.400, "Chest opening sound (end)"),
                            (0.400, "Chest is open and empty (start)"),
                            (2.000, "Chest is open and empty (end)"),
                            (2.000, "Coin is visible (start)"),
                            (2.000, "Coin presentation sound (start)"),
                            (2.650, "Coin presentation sound (end)"),
                            (3.000, "Coin is released for collection.")
                        ]

#### IE Coin Collect 
## Example Trigger Message: "coin collected:1"
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