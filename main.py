import pandas as pd
from llama_api import Llama


def main():
    llama = Llama()

    cur_chain_breakdown = pd.json_normalize(llama.get_cur_tvl_breakdown())

    # Drop non-EVM chains
    cur_chain_breakdown = cur_chain_breakdown[cur_chain_breakdown['chainId'].notnull()]

    # Get first 50 chains sorted by TVL
    cur_chain_breakdown = cur_chain_breakdown.sort_values(by='tvl', ascending=False). \
        head(50).reset_index().drop('index', axis=1)

    cur_chain_breakdown['cur_rank'] = cur_chain_breakdown['tvl'].rank(ascending=False)

    data_points = 1636502400  # 2021/11/10 BTC ATH
    ath_chain_breakdown = cur_chain_breakdown[['name', 'cur_rank']].copy()

    # get the tvl of those chain during BTC ATH
    ath_chain_breakdown['hist_tvl'] = ath_chain_breakdown.apply(
        lambda x: llama.get_hist_chain_tvl(x['name'], data_points),
        axis=1)

    # Ensure chain already launched
    ath_chain_breakdown = ath_chain_breakdown[ath_chain_breakdown['hist_tvl'] > 0.0]
    ath_chain_breakdown['ath_rank'] = ath_chain_breakdown['hist_tvl'].rank(ascending=False)

    ath_chain_breakdown['rank_changes'] = ath_chain_breakdown.apply(lambda x: x['ath_rank'] - x['cur_rank'], axis=1)
    ath_chain_breakdown = ath_chain_breakdown[['name', 'ath_rank', 'cur_rank', 'rank_changes']]
    ath_chain_breakdown.to_csv('chains.csv')


if __name__ == '__main__':
    main()
