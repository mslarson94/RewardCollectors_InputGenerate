
import pandas as pd

def score_po_votes(po_timeline):
    for idx, row in po_timeline.iterrows():
        if row['event_type'] == 'PO Pin Vote':
            vote = row['details'].get('POVote')
            truth = row['details'].get('GoodDrop')
            if vote == "did not vote":
                row['details']['Score'] = "POdidntVote"
            elif vote == truth:
                row['details']['Score'] = "POisCorrect"
            else:
                row['details']['Score'] = "POisIncorrect"
    return po_timeline

def classify_coin_type(coinset_id, coinpoint_id):
    if coinset_id == 2 and coinpoint_id == 2:
        return "PPE"
    elif coinset_id == 3 and coinpoint_id == 0:
        return "NPE"
    elif coinset_id == 1:
        return "Normal"
    elif coinset_id == 2 and coinpoint_id in [0, 1]:
        return "Normal"
    elif coinset_id == 3 and coinpoint_id in [1, 2]:
        return "Normal"
    return "Unknown"

def score_swap_votes(timeline):
    for idx, row in timeline.iterrows():
        if row['event_type'] == "SwapVote":
            coinset_id = row['details'].get("CoinSetID")
            swapvote = row['details'].get("SwapVote")
            score = None
            if coinset_id in [2, 3] and swapvote == "NEW":
                score = "Correct"
            elif coinset_id == 1 and swapvote == "OLD":
                score = "Correct"
            elif coinset_id == 1 and swapvote == "NEW":
                score = "Incorrect"
            elif coinset_id in [2, 3] and swapvote == "OLD":
                score = "Incorrect"
            row['details']["SwapVoteScore"] = score
    return timeline

def run_scoring(po_timeline_path, an_timeline_path=None, output_path="scored_po_timeline.csv"):
    po_df = pd.read_csv(po_timeline_path, converters={'details': eval})
    po_df = score_po_votes(po_df)
    po_df = score_swap_votes(po_df)
    po_df.to_csv(output_path, index=False)
