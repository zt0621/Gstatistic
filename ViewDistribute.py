# coding:utf-8
# This module is used for distribution view

import pandas as pd
import time
import datetime


class ViewDistribute(object):

    def __init__(self, save_name='distribute', cut_quantiles=[]):
        self.__save_name = save_name
        if not cut_quantiles:
            self.__cut_quantiles = [0, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1]
        else:
            self.__cut_quantiles = cut_quantiles

        self.__cut_values = []

    def get_every_distribute(self, input_data, every_col, col_label="", has_label=False):
        # 计算一个变量的分布情况，
        # 使用传入的 分位数 list ，计算离散区间，区间样本数。如果带label，同时将预期情况计算出来
        if not has_label:
            cp_data = input_data[[every_col]]
        else:
            cp_data = input_data[[every_col, col_label]]
        cp_data_not_null = cp_data[~cp_data[every_col].isnull()].reset_index(drop=True)
        cp_data_null = cp_data[cp_data[every_col].isnull()].reset_index(drop=True)
        data_cut, bins = pd.qcut(cp_data_not_null[every_col], q=self.__cut_quantiles, duplicates='drop', retbins=True)
        data_cut_bin, _ = pd.qcut(cp_data_not_null[every_col], q=self.__cut_quantiles, duplicates='drop', retbins=True,
                                  labels=range(len(bins) - 1))

        cp_data_not_null['cuts'] = data_cut
        cp_data_not_null['bins'] = data_cut_bin
        cp_data_not_null['count'] = 1
        if not has_label:
            cut_df_dp = cp_data_not_null.groupby(['bins', 'cuts'], as_index=False).agg({'count': "count"})
        else:
            cut_df_dp = cp_data_not_null.groupby(['bins', 'cuts'], as_index=False).agg(
                {'count': "count", col_label: "sum"})

        cut_df_dp.index = [every_col] + [''] * (cut_df_dp.index.size - 1)
        cut_df_dp = cut_df_dp.reset_index()

        if not has_label:
            cut_df_dp.columns = ['column_name', 'bin_num', 'bin_interval', 'count']
            cut_df_dp.loc[cut_df_dp.index.size] = ['', cut_df_dp.index.size, 'NULL', cp_data_null.index.size]
        else:
            cut_df_dp.columns = ['column_name', 'bin_num', 'bin_interval', 'count', 'overdue_count']
            cut_df_dp.loc[cut_df_dp.index.size] = ['', cut_df_dp.index.size, 'NULL', cp_data_null.index.size,
                                                   cp_data_null[col_label].sum()]

        cut_df_dp['percent'] = cut_df_dp['count'] / cut_df_dp['count'].sum()
        cut_df_dp['percent'] = cut_df_dp['percent'].map(lambda x: round(x, 4))
        if has_label:
            cut_df_dp['overdue_rate'] = cut_df_dp['overdue_count'] / cut_df_dp['count']
            cut_df_dp['overdue_rate'] = cut_df_dp['overdue_rate'].map(lambda x: round(x, 4))

        return cut_df_dp

    def distribute(self, input_data, view_columns, col_label="", has_label=False):
        # 计算每个变量的分布情况，如果传入label，也将逾期率计算出来
        distribute_df = pd.DataFrame()
        for every_col in view_columns:
            cut_df_dp = self.get_every_distribute(input_data, every_col, col_label, has_label)
            cut_df_dp.loc[''] = [''] * cut_df_dp.columns.size
            distribute_df = pd.concat([distribute_df, cut_df_dp])
        return distribute_df

    def distribute_by_group(self, ):
        # 对整个传入的数据列，按照某种分组情况计算分组的分布情况，可用于计算psi, 对比woe
        pass



if __name__ == "__main__":
    data = pd.read_csv("data/data.csv")
    columns = ['col_1', 'col_2', 'col_3', 'col_4', 'col_5']
    # print(data)
    vd = ViewDistribute()
    distribute = vd.distribute(data, columns, has_label=False, col_label='label')
    print(distribute)