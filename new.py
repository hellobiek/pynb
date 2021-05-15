import pandas as pd
import quartz as btr
from quartz.api import StockUniverse
from quartz.account.account import AccountConfig
from quartz.utils.special_trading_days import Monthly


two_year_return = pd.read_excel('./data/under_value.xlsx')
two_year_return['stock_code'] = two_year_return['stock_code'].apply(lambda x : x[:-3]+'.XSHG' if x[0]=='6' else x[:-3]+'.XSHE')
target_stocks = pd.DataFrame(columns=['date','industry_1','industry_2','stock_code','stock_name',
                                      'indicator','indicator_percent','pred_indicator_percent', 'cagr'])

target_stocks = target_stocks.drop(target_stocks.index)


def get_target_comp_return(df):
    df = df.copy()
    if len(df) >= 0:
        df.sort_values('median_pred_return', ascending=False, inplace=True)
        return df.iloc[:int(len(df) / 3)]


def get_target_comp_capex(df):
    df = df.copy()
    if len(df) >= 0:
        df.sort_values('rank_sum', ascending=True, inplace=True)
        return df.iloc[:int(len(df) / 2)]


start = '2016-05-01'
end = '2021-04-18'

benchmark = 'HS300'
universe = StockUniverse('HS300')  # 证券池，支持股票和基金、期货

freq = 'd'
refresh_rate = Monthly(1)

accounts = {
    'stock_account': AccountConfig(account_type='security', capital_base=10000000)
}

# 把回测参数封装到SimulationParameters中，供Backtest使用

sim_params = btr.SimulationParameters(start, end, benchmark, universe, refresh_rate=refresh_rate, accounts=accounts,
                                      freq=freq)

# ---------------回测参数部分结束----------------
# 获取回测行情数据

data = btr.get_backtest_data(sim_params)

import pdb
pdb.set_trace()

# 运行结果
results = {}
# -----策略定义开始，这和常用的策略编写模式完全一样-----
def initialize(context):
    pass


def handle_data(context):
    if context.current_date.month != 5:
        return None
    current_year = context.current_date.year

    all_universe = context.get_universe('stock', exclude_halt=True)
    current_universe = all_universe.copy()
    print('\n', context.current_date)

    current_two_year_return = two_year_return[(two_year_return['year'] == current_year) & \
                                              (two_year_return['month'] == context.current_date.month) & \
                                              (two_year_return['stock_code'].isin(current_universe))].copy()
    print('stock_num: ', current_two_year_return.shape[0], end=' -> ')

    # 低估值
    current_two_year_return_1 = current_two_year_return.groupby('industry_name_1', as_index=False,
                                                                group_keys=False).apply(get_target_comp_return)
    print(current_two_year_return_1.shape[0], end=' -> ')
    current_two_year_return_1 = current_two_year_return_1[
        current_two_year_return_1['price_industry_indicator_corr'] >= 0.6]
    print(current_two_year_return_1.shape[0], end=' -> ')

    current_two_year_return_1 = current_two_year_return_1[current_two_year_return_1['median_pred_return'] >= 10]
    print(current_two_year_return_1.shape[0])

    target_stock_code_list = set(current_two_year_return_1['stock_code'].tolist())
    target_stock_name_list = set(current_two_year_return_1['stock_name'].tolist())

    global target_stocks
    for stock_code in target_stock_code_list:
        single_stock = current_two_year_return[current_two_year_return['stock_code'] == stock_code]
        target_stocks = target_stocks.append({'date': context.current_date,
                                              'industry_1': single_stock['industry_name_1'].values[0],
                                              'industry_2': single_stock['industry_name_2'].values[0],
                                              'stock_code': single_stock['stock_code'].values[0],
                                              'stock_name': single_stock['stock_name'].values[0],
                                              'indicator': single_stock['industry_indicator'].values[0],
                                              'indicator_percent': single_stock['indicator_percent'].values[0],
                                              'pred_indicator_percent': single_stock['pred_indicator_percent'].values[
                                                  0],
                                              'cagr': single_stock['cagr'].values[0]},
                                             ignore_index=True,
                                             sort=True)

    print('本期target的股票列表')
    print(target_stock_name_list)

    print('开始账户操作')

    account = context.get_account('stock_account')

    current_position = account.get_positions(exclude_halt=True)
    print('当前持仓可交易')
    print(set(current_position))
    print('当前持仓全部包括')
    print(set(account.get_positions(exclude_halt=False)))

    for stock in set(current_position).difference(target_stock_code_list):
        account.order_to(stock, 0)

    for stock in target_stock_code_list:
        # 根据市值下单
        #         account.order_pct_to(stock,  hist['MktValue'].loc[stock] / hist['MktValue'].sum())
        account.order_pct_to(stock, 1 / len(target_stock_code_list))

# 生成策略对象
#strategy = btr.TradingStrategy(initialize, handle_data)
#bt, perf = btr.backtest(sim_params, strategy, data=data)

# 开始回测

data_dict = sim_params.to_dict()

bt, perf, account = btr.backtest(start=data_dict['start'],
                        end=data_dict['end'],
                        benchmark=data_dict['major_benchmark'],
                        universe=data_dict['universe'],
                        capital_base=data_dict['capital_base'],
                        refresh_rate=data_dict['refresh_rate'],
                        freq=data_dict['freq'],
                        accounts=data_dict['accounts'],
                        initialize=initialize,
                        handle_data=handle_data,
                        preload_data=data)


print('\n策略年化收益率：', format(perf['annualized_return'], '.2%'))
print('基准年化收益率：', format(perf['benchmark_annualized_return'], '.2%'))
print('策略累计收益率：', format(perf['cumulative_returns'].values[-1], '.2%'))
print('基准累计收益率：', format(perf['benchmark_cumulative_returns'].values[-1], '.2%'))
#
#target_stocks = target_stocks[['date', 'industry_1', 'industry_2', 'stock_code', 'stock_name',
#                               'indicator', 'indicator_percent', 'pred_indicator_percent', 'cagr']]
#target_stocks.to_excel('target_stocks_5.xlsx')