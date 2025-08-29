
def process_block_periods(df):
    events = []
    for row in df.itertuples():
        msg = row.Message if isinstance(row.Message, str) else ""

        if msg == "Mark should happen if checked on terminal.":
            events.append({
                "AppTime": row.AppTime,
                "Timestamp": row.Timestamp,
                "event_type": "PreBlock_BlueCylinderVisible_start",
                "details": {},
                "source": "synthetic",
                "original_row": row.Index
            })

        elif msg == "Repositioned and ready to start block or round":
            events.extend([
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "event_type": "PreBlock_BlueCylinderVisible_end",
                    "details": {},
                    "source": "synthetic",
                    "original_row": row.Index
                },
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "event_type": "StartRoundText_visible_start",
                    "details": {},
                    "source": "synthetic",
                    "original_row": row.Index
                }
            ])

        elif msg.startswith("Started"):
            events.extend([
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "event_type": "StartRoundText_visible_end",
                    "details": {},
                    "source": "synthetic",
                    "original_row": row.Index
                },
                {
                    "AppTime": row.AppTime,
                    "Timestamp": row.Timestamp,
                    "event_type": "RoundInstructionText_visible_start",
                    "details": {},
                    "source": "synthetic",
                    "original_row": row.Index
                },
                {
                    "AppTime": row.AppTime + 2.0,
                    "Timestamp": row.Timestamp,  # Approximation for local time
                    "event_type": "RoundInstructionText_visible_end",
                    "details": {},
                    "source": "synthetic",
                    "original_row": row.Index
                }
            ])
    return events
