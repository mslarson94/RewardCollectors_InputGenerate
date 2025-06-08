def process_TrueBlocks(df, allowed_statuses, cascade_windows=None):

    events = []
    for idx, row in df.iterrows():
        if isinstance(row.Message, str) and (
            row.Message.startswith("Started collecting.") or
            row.Message.startswith("Started pindropping.") or
            row.Message.startswith("Started watching other participant's collecting.") or
            row.Message.startswith("Started watching other participant's pin dropping.")
        ):
            common_fields = build_common_event_fields(row, idx)
            start_time = safe_parse_timestamp(row.Timestamp)
            block_start_event = {
                **common_fields,
                "AppTime": start_time,
                "Timestamp": start_ts,
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": start_ts,
                "lo_eventType": "TrueBlockStart",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {},
                "source": "logged",
            }
            events.append(block_start_event)

        elif isinstance(row.Message, str) and "finished current task" in row.Message:
            common_info = build_common_event_fields(row, idx)
            matched = match_cascade_window(row, cascade_windows) if cascade_windows else None

            start_time = row["AppTime"]
            timestamp = row["Timestamp"]

            start_ts_dt = safe_parse_timestamp(timestamp)
            start_ts = start_ts_dt.time().strftime('%H:%M:%S:%f') if start_ts_dt else None
            if start_ts is None:
                print(f"⚠️ base_timestamp is None for input: {timestamp} with base_info: {common_info}")

            #details = {"mark": "A"} if role == "AN" else {"mark": "B"}

            events.append({
                "AppTime": start_time,
                "Timestamp": start_ts,
                "start_AppTime": start_time,
                "end_AppTime": start_time,
                "start_Timestamp": start_ts,
                "end_Timestamp": start_ts,
                "lo_eventType": "TrueBlockEnd",
                "med_eventType": "ReferencePoint",
                "hi_eventType": "SystemEvent",
                "hiMeta_eventType": "Infrastructure",
                "details": {},
                "source": "logged",
                **common_info
            })

    return events