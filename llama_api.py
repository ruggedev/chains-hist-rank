import pandas as pd
import helpers.request as rq
import helpers.utils as utils
import helpers.date as date_helper


class Llama:
    base_prefix = 'https://api.llama.fi'
    coins_prefix = 'https://coins.llama.fi'

    @staticmethod
    # Overriding the original get
    def get(endpoint, col=None):
        """
        :param endpoint:
        :param col: columns to return
        :return:
        """
        return rq.get(endpoint, 'defillama', col)

    def get_url(self, endpoint):
        return f'{self.base_prefix}/{endpoint}'

    def get_coin_url(self, endpoint):
        return f'{self.coins_prefix}/{endpoint}'

    def get_cur_tvl_breakdown(self):
        endpoint = self.get_url('v2/chains')
        return self.get(endpoint=endpoint)

    def get_hist_chain_tvl(self, chain_slug, timestamp):
        endpoint = self.get_url(f'v2/historicalChainTvl/{chain_slug}')
        df = pd.json_normalize(self.get(endpoint=endpoint))
        tvl = utils.get_value_from_df(df, 'date', timestamp, 'tvl')
        return tvl or 0.0

    @staticmethod
    def parse_llama_res(llama_res, key):
        if key in llama_res:
            return llama_res[key]
        else:
            return None