import pandas as pd

def add_block_repeat_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Ensure types
    out["BlockInstance"] = pd.to_numeric(out["BlockInstance"], errors="coerce").astype("Int64")
    out["BlockNum"] = pd.to_numeric(out["BlockNum"], errors="coerce").astype("Int64")

    # One row per block instance: take the first non-null BlockNum within that block instance
    blocks = (
        out.sort_index()
           .groupby("BlockInstance", dropna=False)["BlockNum"]
           .apply(lambda s: s.dropna().iloc[0] if s.dropna().size else pd.NA)
           .reset_index()
           .rename(columns={"BlockNum": "BlockNum_first"})
    )
    blocks["BlockNum_first"] = blocks["BlockNum_first"].astype("Int64")

    # Count repeats within BlockNum in chronological order of BlockInstance
    blocks = blocks.sort_values("BlockInstance").reset_index(drop=True)
    blocks["BlockRepeatIndex"] = (
        blocks.groupby("BlockNum_first", dropna=False).cumcount() + 1
    ).astype("Int64")

    # Merge back to all rows by BlockInstance
    out = out.merge(blocks[["BlockInstance", "BlockRepeatIndex"]], on="BlockInstance", how="left")

    return out


import pandas as pd

def add_block_repeat_index_to_events(events: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()

    # normalize types
    for c in ["BlockInstance", "BlockNum"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
    out["start_AppTime"] = pd.to_numeric(out.get("start_AppTime"), errors="coerce")

    # block starts table
    bs = out[out["lo_eventType"].astype(str) == "BlockStart"].copy()

    # one row per block occurrence
    bs = bs.drop_duplicates(["BlockInstance", "BlockNum"]).copy()

    # chronological repeat count within BlockNum
    bs = bs.sort_values(["BlockNum", "start_AppTime", "BlockInstance"])
    bs["BlockRepeatIndex"] = (bs.groupby("BlockNum").cumcount() + 1).astype("Int64")

    # merge back to all event rows
    out = out.merge(
        bs[["BlockInstance", "BlockNum", "BlockRepeatIndex"]],
        on=["BlockInstance", "BlockNum"],
        how="left",
        validate="m:1",
    )
    return out
