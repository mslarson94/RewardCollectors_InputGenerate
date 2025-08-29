
# --- Utility: Coin and Swap Vote Classification ---
def classify_coin_type(CoinSetID, idvCoinID):
    if CoinSetID == 2 and idvCoinID == 2:
        return "PPE"
    elif CoinSetID == 3 and idvCoinID == 0:
        return "NPE"
    elif CoinSetID == 1 or (CoinSetID in [2, 3] and idvCoinID in [0, 1]):
        return "Normal"
    elif CoinSetID == 4 or (CoinSetID == 5 and idvCoinID == 1):
        return "TutorialNorm"
    elif CoinSetID == 5 and idvCoinID in [0, 2]:
        return "TutorialRPE"
    return "Unknown"

def classify_swap_vote(CoinSetID, swapvote):
    if CoinSetID in [2, 3] and swapvote == "NEW":
        return "Correct"
    elif CoinSetID == 1 and swapvote == "OLD":
        return "Correct"
    elif CoinSetID == 1 and swapvote == "NEW":
        return "Incorrect"
    elif CoinSetID in [2, 3] and swapvote == "OLD":
        return "Incorrect"
    return "Unknown"