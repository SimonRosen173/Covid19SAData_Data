print("Imports started")

import pandas as pd
import numpy as np
import io
import requests
from datetime import timedelta, datetime
import os
from dateutil import tz
import sys
import shutil

print("Imports successful")

scraped_data_path = 'data/scraped/'


# get dataframe from specified url using kwargs specified for read_csv
def df_from_url(df_url, pd_kwargs={}, use_base_url=True):
    base_url = "https://raw.githubusercontent.com/dsfsi/covid19za/master/data/"
    if use_base_url:
        df_url = base_url + df_url
    df_req = requests.get(df_url).content
    df = pd.read_csv(io.StringIO(df_req.decode('utf-8')), **pd_kwargs)
    return df


# Generator method to get all dates in specified interval
def datetime_range(start_datetime, end_datetime):
    curr_date = start_datetime
    yield curr_date
    while curr_date < end_datetime:
        curr_date += timedelta(days=1)
        yield curr_date


# ---------------
#   SOUTH AFRICA
#  COUNTRY LEVEL
# ---------------
# Data at country level
def preprocess_sa_data():
    def get_cum_daily(data_url, cum_col='total', index_col='date', use_local_data=False):  # kwargs={}):
        cols = ['date', 'total']
        pd_kwargs = {"usecols": [cum_col, index_col, 'source'], "index_col": [index_col]}

        if use_local_data:
            path = scraped_data_path + data_url
            data = pd.read_csv(path, **pd_kwargs)
        else:
            data = df_from_url(data_url, pd_kwargs)

        data.reset_index(inplace=True)
        data['date'] = pd.to_datetime(data['date'], format='%d-%m-%Y')
        data.set_index('date', inplace=True)
        data.rename({cum_col: "cum_no"}, axis=1, inplace=True)
        data.ffill(inplace=True)

        data['daily_no'] = data['cum_no']
        data['daily_no'][1:] = data['cum_no'].diff()[1:]

        # get rolling averages
        data['daily_no_rol_avg_3'] = round(data['daily_no'].rolling(3).mean())
        data['daily_no_rol_avg_7'] = round(data['daily_no'].rolling(7).mean())

        # Cast columns to integer
        data[['cum_no']] = data[['cum_no']].astype('int32')
        return data

    confirmed_cases_url = "covid19za_provincial_cumulative_timeline_confirmed.csv"
    confirmed_data = get_cum_daily(confirmed_cases_url, use_local_data=True)

    deaths_url = "covid19za_provincial_cumulative_timeline_deaths.csv"
    deaths_data = get_cum_daily(deaths_url, use_local_data=True)

    tests_url = "covid19za_timeline_testing.csv"
    tests_data = get_cum_daily(tests_url, 'cumulative_tests', 'date', use_local_data=True)

    recovered_url = "covid19za_provincial_cumulative_timeline_recoveries.csv"
    # recovered_data = get_cum_daily(tests_url, 'recovered', 'date')
    recovered_data = get_cum_daily(recovered_url, use_local_data=True)

    def get_active_cases():
        _active_data = confirmed_data[['cum_no']].copy().rename({"cum_no": "confirmed"}, axis=1)
        _active_data = pd.concat([_active_data,
                                  recovered_data[['cum_no']].copy().rename({"cum_no": "recovered"}, axis=1),
                                  deaths_data[['cum_no']].copy().rename({"cum_no": "deaths"}, axis=1)
                                  ], axis=1)
        _active_data = _active_data.iloc[9:]
        _active_data = _active_data.ffill().fillna(0)

        _active_data['cum_no'] = _active_data['confirmed'] - _active_data['recovered'] - _active_data['deaths']
        _active_data.drop(['confirmed', 'recovered', 'deaths'], axis=1, inplace=True)
        _active_data['daily_no'] = _active_data['cum_no'].copy()
        _active_data['daily_no'].iloc[1:] = _active_data['cum_no'].diff().iloc[1:]

        # rolling average
        _active_data['daily_no_rol_avg_3'] = _active_data['daily_no'].rolling(3).mean()
        _active_data['daily_no_rol_avg_7'] = _active_data['daily_no'].rolling(7).mean()

        _active_data[['cum_no', 'daily_no']] = _active_data[['cum_no', 'daily_no']].astype('int32')

        return _active_data

    active_data = get_active_cases()

    def get_all_cum_data():
        _all_cum_data = confirmed_data[['cum_no']].rename({"cum_no": "confirmed"}, axis=1)
        _all_cum_data = pd.concat([
            _all_cum_data,
            tests_data[['cum_no']].rename({"cum_no": "tests"}, axis=1),
            deaths_data[['cum_no']].rename({"cum_no": "deaths"}, axis=1),
            recovered_data[['cum_no']].rename({"cum_no": "recovered"}, axis=1),
            active_data[['cum_no']].rename({"cum_no": "active"}, axis=1),

        ], axis=1)
        # _all_cum_data['recovered'] = recovered_data['cum_no']
        # _all_cum_data['active'] = active_data['cum_no']
        #     _all_cum_data.ffill(inplace=True)
        #     _all_cum_data.fillna(0, inplace=True)
        #     _all_cum_data = _all_cum_data.astype('int32')
        _all_cum_data = _all_cum_data.round()

        # DERIVED STATS

        # confirmed_div_by_tests
        _all_cum_data['confirmed_div_by_tests'] = _all_cum_data['confirmed'] / _all_cum_data['tests']
        _all_cum_data['confirmed_div_by_tests'] = _all_cum_data['confirmed_div_by_tests'].round(3)

        # deaths_div_by_confirmed
        _all_cum_data['deaths_div_by_confirmed'] = _all_cum_data['deaths'] / _all_cum_data['confirmed']
        _all_cum_data['deaths_div_by_confirmed'] = _all_cum_data['deaths_div_by_confirmed'].round(3)
        #     _all_cum_data.fillna(0.000, inplace=True)

        # recovered_div_by_confirmed
        _all_cum_data['recovered_div_by_confirmed'] = _all_cum_data['recovered'] / _all_cum_data['confirmed']
        _all_cum_data['recovered_div_by_confirmed'] = _all_cum_data['recovered_div_by_confirmed'].round(3)
        #     _all_cum_data.fillna(0.000, inplace=True)

        # STATS PER MILLION POP

        sa_tot_population = 59195720
        # total population rounded in millions
        sa_tot_pop_mil = sa_tot_population / 1000000

        _all_cum_data['confirmed_per_mil'] = _all_cum_data['confirmed'] / sa_tot_pop_mil
        _all_cum_data['tests_per_mil'] = _all_cum_data['tests'] / sa_tot_pop_mil
        _all_cum_data['deaths_per_mil'] = _all_cum_data['deaths'] / sa_tot_pop_mil
        _all_cum_data['recovered_per_mil'] = _all_cum_data['recovered'] / sa_tot_pop_mil
        _all_cum_data['active_per_mil'] = _all_cum_data['active'] / sa_tot_pop_mil
        tmp_cols = ['confirmed_per_mil', 'tests_per_mil', 'deaths_per_mil', 'recovered_per_mil', 'active_per_mil']
        _all_cum_data[tmp_cols] = _all_cum_data[tmp_cols].round(2)
        #     _all_cum_data.fillna(0.00, inplace=True)

        return _all_cum_data

    # All cumulative data
    all_cum_data = get_all_cum_data()
    all_cum_data.to_csv('data/sa/all_cum_data.csv')

    def get_all_daily_data():
        #     _all_daily_data = confirmed_data[['daily_no']].rename({"daily_no": "confirmed"}, axis=1)
        cols_to_use = ['daily_no', 'daily_no_rol_avg_3', 'daily_no_rol_avg_7']
        val_names = ["confirmed", "tests", "deaths", "recovered", "active"]

        def rename_df(df, val_name):
            col_orig_names = cols_to_use
            new_df = df[col_orig_names].copy()
            col_new_suffixes = ['', '_rol_avg_3', '_rol_avg_7']
            rename_dict = {orig_name: val_name + new_suffix for (orig_name, new_suffix) in
                           zip(col_orig_names, col_new_suffixes)}
            #         print(rename_dict)
            new_df.rename(rename_dict, axis=1, inplace=True)
            return new_df

        _all_daily_data = rename_df(confirmed_data, 'confirmed')

        _all_daily_data = pd.concat([
            _all_daily_data,
            #         tests_data[cols_to_use].rename({"daily_no": "tests"}, axis=1),
            #         deaths_data[cols_to_use].rename({"daily_no": "deaths"}, axis=1),
            #         recovered_data[cols_to_use].rename({"daily_no": "recovered"}, axis=1),
            #         active_data[cols_to_use].rename({"daily_no": "active"}, axis=1),
            rename_df(tests_data[cols_to_use], 'tests'),
            rename_df(deaths_data[cols_to_use], 'deaths'),
            rename_df(recovered_data[cols_to_use], 'recovered'),
            rename_df(active_data[cols_to_use], 'active'),

        ], axis=1)
        return _all_daily_data

    # All daily data
    all_daily_data = get_all_daily_data()
    all_daily_data.to_csv("data/sa/all_daily_data.csv")

    def get_index_page_data():
        def zero_space_format(num):
            is_pos = True if num >= 0 else False
            num = zero_space(num)
            if is_pos:
                num = "+" + num
            else:
                num = "-" + num

            return num

        def zero_space(num):
            return format(num, ',d').replace(",", " ")

        def format_date(date: datetime) -> str:
            return date.strftime("%d/%m/%Y")

        # Tests
        # tot_tested = zero_space(tests_data.iloc[-1]['cum_no'].astype(int))
        # change_tested = zero_space_format(tests_data.iloc[-1]['daily_no'].astype(int))
        tot_tested = tests_data.iloc[-1]['cum_no'].astype(int)
        change_tested = tests_data.iloc[-1]['daily_no'].astype(int)
        last_date_tested = format_date(tests_data.index[-1])
        second_last_date_tested = format_date(tests_data.index[-2])
        sources_tested = tests_data.iloc[-1]['source'] + "," + tests_data.iloc[-2]['source']

        # Confirmed
        # tot_confirmed = zero_space(confirmed_data.iloc[-1]['cum_no'].astype(int))
        # change_confirmed = zero_space_format(confirmed_data.iloc[-1]['daily_no'].astype(int))
        tot_confirmed = confirmed_data.iloc[-1]['cum_no'].astype(int)
        change_confirmed = confirmed_data.iloc[-1]['daily_no'].astype(int)
        last_date_confirmed = format_date(confirmed_data.index[-1])
        second_last_date_confirmed = format_date(confirmed_data.index[-2])
        sources_confirmed = confirmed_data.iloc[-1]['source'] + "," + confirmed_data.iloc[-2]['source']

        # Active
        # tot_active = zero_space(active_data.iloc[-1]['cum_no'].astype(int))
        # change_active = zero_space_format(active_data.iloc[-1]['daily_no'].astype(int))
        tot_active = active_data.iloc[-1]['cum_no'].astype(int)
        change_active = active_data.iloc[-1]['daily_no'].astype(int)
        last_date_active = format_date(confirmed_data.index[-1])
        second_last_date_active = format_date(confirmed_data.index[-2])

        # Deaths
        # tot_deaths = zero_space(deaths_data.iloc[-1]['cum_no'].astype(int))
        # change_deaths = zero_space_format(deaths_data.iloc[-1]['daily_no'].astype(int))
        tot_deaths = deaths_data.iloc[-1]['cum_no'].astype(int)
        change_deaths = deaths_data.iloc[-1]['daily_no'].astype(int)
        last_date_deaths = format_date(deaths_data.index[-1])
        second_last_date_deaths = format_date(deaths_data.index[-2])
        sources_deaths = deaths_data.iloc[-1]['source'] + "," + deaths_data.iloc[-2]['source']

        # Recoveries
        # tot_recoveries = zero_space(recovered_data.iloc[-1]['cum_no'].astype(int))
        # change_recoveries = zero_space_format(recovered_data.iloc[-1]['daily_no'].astype(int))
        tot_recoveries = recovered_data.iloc[-1]['cum_no'].astype(int)
        change_recoveries = recovered_data.iloc[-1]['daily_no'].astype(int)
        last_date_recoveries = format_date(recovered_data.index[-1])
        second_last_date_recoveries = format_date(recovered_data.index[-2])
        sources_recoveries = recovered_data.iloc[-1]['source'] + "," + recovered_data.iloc[-2]['source']

        now = datetime.now()
        current_time = now.strftime("%H:%M %d %B %Y")

        _gen_data = pd.DataFrame(dict(
            tot_confirmed=[tot_confirmed], change_confirmed=[change_confirmed], last_date_confirmed=[last_date_confirmed],
            second_last_date_confirmed=[second_last_date_confirmed], sources_confirmed=[sources_confirmed],

            tot_deaths=[tot_deaths], change_deaths=[change_deaths],  last_date_deaths=[last_date_deaths],
            second_last_date_deaths=[second_last_date_deaths], sources_deaths=[sources_deaths],

            tot_active=[tot_active], change_active=[change_active], last_date_active=[last_date_active],
            second_last_date_active=[second_last_date_active],

            tot_tests=[tot_tested], change_tests=[change_tested], last_date_tests=[last_date_tested],
            second_last_date_tests=[second_last_date_tested], sources_tests=[sources_tested],

            tot_recoveries=[tot_recoveries], change_recoveries=[change_recoveries], last_date_recoveries=[last_date_recoveries],
            second_last_date_recoveries=[second_last_date_recoveries], sources_recoveries=[sources_recoveries],

            processed_datetime=[current_time]))

        return _gen_data

    index_page_data = get_index_page_data()
    index_page_data.to_csv("data/sa/gen_data.csv", index=False)

    # save datetime when sa data page was last updated
    # convert datetime to SA time zone
    curr_zone = tz.tzlocal()
    sa_zone = tz.gettz('Africa/Johannesburg')

    now_datetime_curr_zone = datetime.now().replace(tzinfo=curr_zone)
    now_datetime_sa_zone = now_datetime_curr_zone.astimezone(sa_zone)

    # if tz.tolocal() == tz.tzutc():
    #     utc_zone = tz.gettz('UTC')
    #     sa_zone = tz.gettz('Africa/Johannesburg')
    #
    #     now_datetime_utc = datetime.now().replace(tzinfo=utc_zone)
    #
    #     now_datetime_sa = now_datetime_utc.astimezone(to_zom)
    # else:
    #     now_datetime_sa = datetime.now()

    # now_datetime_gmt = now_datetime_utc
    sa_page_updated = now_datetime_sa_zone.strftime("%d %B %Y @ %I:%M%p")

    data_info_df = pd.read_csv('data/data_info.csv', index_col='name')
    data_info_df.loc['sa_page_updated']['date_updated'] = sa_page_updated
    data_info_df.to_csv('data/data_info.csv')


# -----------
# BY PROVINCE
# -----------
def preprocess_prov_data():
    from datetime import timedelta

    # Generator method to get all dates in specified interval
    def datetime_range(start_datetime, end_datetime):
        curr_date = start_datetime
        yield curr_date
        while curr_date < end_datetime:
            curr_date += timedelta(days=1)
            yield curr_date

    # round_no - decimals to round to
    # todo
    def get_cum_daily_by_prov(data_url, fill_date_gaps=False, dropna=True, round_no=3):
        cols = ['date', 'EC', 'FS', 'GP', 'KZN', 'LP', 'MP', 'NC', 'NW', 'WC', 'UNKNOWN']
        pd_kwargs = {"usecols": cols}
        cum_data = df_from_url(data_url, pd_kwargs)

        if dropna:
            cum_data.dropna(inplace=True)

        cum_data['date'] = pd.to_datetime(cum_data['date'], format='%d-%m-%Y')

        if fill_date_gaps:
            start_date = cum_data.iloc[0]['date']
            end_date = cum_data.iloc[-1]['date']
            date_range = list(datetime_range(start_date, end_date))
            cum_data.set_index('date', inplace=True)
            cum_data = cum_data.reindex(date_range)
            cum_data.ffill(inplace=True)
            cum_data.reset_index(inplace=True)

        daily_data = cum_data.copy()
        daily_data.iloc[1:, 1:] = daily_data.iloc[:, 1:].diff().iloc[1:]
        daily_data_melt = daily_data.melt(id_vars=['date'], var_name='province', value_name='daily_no')
        daily_data_melt.set_index(['date'], inplace=True)

        cum_data_melt = cum_data.melt(id_vars=['date'], var_name='province', value_name='cum_no')
        cum_data_melt.set_index(['date'], inplace=True)

        data = pd.concat([cum_data_melt, daily_data_melt[['daily_no']]], axis=1)
        data[['cum_no', 'daily_no']] = data[['cum_no', 'daily_no']].astype('int32')

        prov_pops = {  # https://github.com/dsfsi/covid19za/blob/master/data/district_data/za_province_pop.csv
            "EC": 6712276.0,
            "FS": 2887465.0,
            "GP": 15176115.0,
            "KZN": 11289086.0,
            "LP": 5982584.0,
            "MP": 4592187.0,
            "NW": 4072160.0,
            "NC": 1263875.0,
            "WC": 6844272.0,
            "UNKNOWN": None
        }

        data['cum_no_perc_pop'] = data['province'].map(prov_pops)
        data['cum_no_perc_pop'] = data['cum_no'] / data['cum_no_perc_pop'] * 100
        data['cum_no_perc_pop'] = data['cum_no_perc_pop'].round(round_no)

        data['daily_no_perc_pop'] = data['province'].map(prov_pops)
        data['daily_no_perc_pop'] = data['daily_no'] / data['daily_no_perc_pop'] * 100
        data['daily_no_perc_pop'] = data['daily_no_perc_pop'].round(round_no)

        return data

    # Confirmed
    # confirmed_by_prov_timeline = get_cum_daily_by_prov("covid19za_provincial_cumulative_timeline_confirmed.csv")
    # confirmed_by_prov_timeline.to_csv("data/provincial/confirmed_by_prov_timeline_flat.csv")
    #
    # # Deaths
    # deaths_by_prov_timeline = get_cum_daily_by_prov("covid19za_provincial_cumulative_timeline_deaths.csv",
    #                                                 round_no=4)
    # deaths_by_prov_timeline.to_csv("data/provincial/deaths_by_prov_timeline_flat.csv")
    #
    # # Recoveries
    # recoveries_by_prov_timeline = get_cum_daily_by_prov("covid19za_provincial_cumulative_timeline_recoveries.csv",
    #                                                     fill_date_gaps=True)
    # recoveries_by_prov_timeline.to_csv("data/provincial/recoveries_by_prov_timeline_flat.csv")

    # Total & Latest Change
    def get_tot_latest_change(data_url, fill_date_gaps=False, use_local_data=False):
        cols = ['date', 'EC', 'FS', 'GP', 'KZN', 'LP', 'MP', 'NC', 'NW', 'WC', 'UNKNOWN']
        pd_kwargs = {"usecols": cols}

        if use_local_data:
            cum_data = pd.read_csv(scraped_data_path+data_url, **pd_kwargs)
        else:
            cum_data = df_from_url(data_url, pd_kwargs)

        cum_data.dropna(inplace=True)  # Rather fillna or ffill - look into
        cum_data['date'] = pd.to_datetime(cum_data['date'], format='%d-%m-%Y')

        if fill_date_gaps:
            start_date = cum_data.iloc[0]['date']
            end_date = cum_data.iloc[-1]['date']
            date_range = list(datetime_range(start_date, end_date))
            cum_data.set_index('date', inplace=True)
            cum_data = cum_data.reindex(date_range)
            cum_data.ffill(inplace=True)
            cum_data.reset_index(inplace=True)

        province_names = {
            "EC": "Eastern Cape",
            "FS": "Free State",
            "GP": "Gauteng",
            "KZN": "KwaZulu-Natal",
            "LP": "Limpopo",
            "MP": "Mpumalanga",
            "NW": "North West",
            "NC": "Northern Cape",
            "WC": "Western Cape",
            "UNKNOWN": "Unknown"
        }

        daily_data = cum_data.copy()
        daily_data.iloc[1:, 1:] = daily_data.iloc[:, 1:].diff().iloc[1:]
        daily_data = daily_data.tail(1)  # get last entry
        daily_data_melt = daily_data.melt(id_vars=['date'], var_name='province', value_name='latest_change')
        daily_data_melt['province'] = daily_data_melt['province'].map(province_names)
        daily_data_melt.set_index(['province'], inplace=True)

        cum_data = cum_data.tail(1)  # get last entry
        cum_data_melt = cum_data.melt(id_vars=['date'], var_name='province', value_name='total')
        cum_data_melt['province'] = cum_data_melt['province'].map(province_names)
        cum_data_melt.set_index(['province'], inplace=True)

        data = pd.concat([cum_data_melt, daily_data_melt[['latest_change']]], axis=1)
        data.drop(['date'], axis=1, inplace=True)
        data = data.astype('int32')

        return data

    # Total & latest change in deaths by prov
    deaths_by_prov_total = get_tot_latest_change("covid19za_provincial_cumulative_timeline_deaths.csv")
    deaths_by_prov_total.to_csv('data/provincial/tot_deaths_provinces.csv')

    # Total & latest change in confirmed by prov
    confirmed_by_prov_total = get_tot_latest_change("covid19za_provincial_cumulative_timeline_confirmed.csv")
    confirmed_by_prov_total.to_csv('data/provincial/tot_provinces.csv')

    # Total & latest change in recoveries by prov
    recoveries_by_prov_total = get_tot_latest_change("covid19za_provincial_cumulative_timeline_recoveries.csv")
    recoveries_by_prov_total.to_csv('data/provincial/tot_recovered_provinces.csv')

    # Total & latest change in tests per prov
    tests_by_prov_total = get_tot_latest_change("covid19za_provincial_cumulative_timeline_testing.csv")
    tests_by_prov_total.to_csv('data/provincial/tot_tests_provinces.csv')

    # Summary of data
    # Province | Confirmed | Change in Confirmed | Recovered | Change in Recovered | Deaths | Change In Deaths
    def get_prov_summary():
        def get_prov_df_correct_format(df, cols):
            new_df = df.copy()
            new_df['latest_change'] = new_df['latest_change'].astype(str)
            new_df.reset_index(inplace=True)

            new_df = new_df.rename({"province": "Province", "total": cols[0], "latest_change": cols[1]}, axis=1, )
            new_df.set_index('Province', inplace=True)
            return new_df

        prov_df_list = [confirmed_by_prov_total, recoveries_by_prov_total, deaths_by_prov_total]

        def add_total(df):
            new_df = df.copy()
            sum_series = new_df.sum()
            sum_series.rename('Total', inplace=True)
            new_df = new_df.append(sum_series)
            return new_df

        prov_df_list = list(map(add_total, prov_df_list))

        prov_df_cols_list = [['tot_confirmed', 'change_confirmed'], ['tot_recoveries', 'change_recoveries'], ['tot_deaths', 'change_deaths']]
        form_prov_df_list = [get_prov_df_correct_format(tup[0], tup[1]) for tup in zip(prov_df_list, prov_df_cols_list)]

        _prov_summary_df = pd.concat([form_prov_df_list[0], form_prov_df_list[1], form_prov_df_list[2]], axis=1)

        _prov_summary_df = _prov_summary_df.apply(pd.to_numeric)
        _prov_summary_df['tot_active'] = _prov_summary_df['tot_confirmed'] - _prov_summary_df['tot_deaths'] - _prov_summary_df['tot_recoveries']
        _prov_summary_df['change_active'] = _prov_summary_df['change_confirmed'] - _prov_summary_df['change_deaths'] - _prov_summary_df['change_recoveries']

        return _prov_summary_df

    prov_summary_df = get_prov_summary()
    prov_summary_df.to_csv("data/provincial/prov_summary.csv")


# ------------
#   GAUTENG
# BY DISTRICT
# ------------
def preprocess_gp_data():
    print("GP Pre-Processing Started")
    import re

    def format_gp_cols(df):
        def change_col_name(col_name):
            new_col_name = re.sub(r'\tCases| |GP', '', col_name)
            new_col_name = new_col_name.replace("Unallocated", "Unknown")
            return new_col_name

        change_col_name_list = list(map(change_col_name, list(df.columns)))

        replace_col_dict = {key: value for (key, value) in zip(list(df.columns), change_col_name_list)}

        df.rename(replace_col_dict, axis=1, inplace=True)

    def get_tot_latest_change_per_district(data_url, province, usecols=[], use_url_prefix=True, use_local_csv=False):
        if usecols == []:
            usecols = [0] + list(range(2, 8))

        pd_kwargs = {"usecols": usecols}

        # Format gp district columns to have desired names i.e. only district name & Unallocated = Unknown

        if use_local_csv:
            cum_data = pd.read_csv("data/source/provincial_gp_cumulative.csv", **pd_kwargs)
        else:
            cum_data = df_from_url(data_url, pd_kwargs, use_base_url=use_url_prefix)

        cum_data.dropna(inplace=True)

        if province == 'gp':
            format_gp_cols(cum_data)
        else:
            raise Exception("Province not supported")

        cum_data['date'] = pd.to_datetime(cum_data['date'], format='%d-%m-%Y')
        last_date = cum_data['date'].iloc[-1]

        daily_data = cum_data.copy()
        daily_data.iloc[1:, 1:] = daily_data.iloc[:, 1:].diff().iloc[1:]
        daily_data = daily_data.tail(1)  # get last entry
        daily_data_melt = daily_data.melt(id_vars=['date'], var_name='district', value_name='latest_change')
        daily_data_melt.set_index(['district'], inplace=True)

        cum_data = cum_data.tail(1)  # get last entry
        cum_data_melt = cum_data.melt(id_vars=['date'], var_name='district', value_name='total')
        cum_data_melt.set_index(['district'], inplace=True)

        data = pd.concat([cum_data_melt, daily_data_melt[['latest_change']]], axis=1)
        data.drop(['date'], axis=1, inplace=True)
        data = data.astype('int32')

        return data, last_date

    gp_tot_latest_df, gp_summary_date = get_tot_latest_change_per_district('district_data/provincial_gp_cumulative.csv', 'gp')
    gp_tot_latest_df.to_csv("data/gp/gp_tot_latest.csv")

    # Additional data for tables & figures, e.g. date which data is for
    sa_page_updated = datetime.now().strftime("%d %B %Y @ %I:%M%p")
    df_dict = {
        "name": [
            "gp_tot_latest",
            "sa_page_updated"
        ],
        "date_updated": [
            gp_summary_date.strftime("%d %B %Y"),
            sa_page_updated
        ]
    }
    data_info_df = pd.DataFrame(df_dict)
    data_info_df.set_index("name")
    data_info_df.to_csv("data/data_info.csv", index=False)

    def get_summary_df():
        _gp_summary = gp_tot_latest_df.reset_index()
        _gp_summary.rename({"district": "District", "total": "Cases", "latest_change": "New Cases"}, axis=1, inplace=True)
        _gp_summary.set_index("District", inplace=True)

        def add_total(df):
            new_df = df.copy()
            sum_series = new_df.sum()
            sum_series.rename('Total', inplace=True)
            new_df = new_df.append(sum_series)
            return new_df

        _gp_summary = add_total(_gp_summary)

        return _gp_summary

    gp_summary = get_summary_df()
    gp_summary.to_csv("data/gp/gp_summary.csv")

    # -----------
    #  OVER TIME
    # -----------
    # round_no - decimals to round to
    def get_cum_daily_by_distict(data_url, usecols=[], fill_date_gaps=False, dropna=True,
                                 round_no=3, use_url_prefix=True, province='gp',
                                 use_local_csv=False):
        if usecols == []:
            usecols = [0] + list(range(2, 8))

        pd_kwargs = {"usecols": usecols}

        if use_local_csv:
            cum_data = pd.read_csv("data/source/provincial_gp_cumulative.csv", **pd_kwargs)
        else:
            cum_data = df_from_url(data_url, pd_kwargs, use_base_url=use_url_prefix)

        if province == 'gp':
            format_gp_cols(cum_data)
        else:
            raise Exception("Province not supported")

        if dropna:
            cum_data.dropna(inplace=True)

        cum_data['date'] = pd.to_datetime(cum_data['date'], format='%d-%m-%Y')

        if fill_date_gaps:
            start_date = cum_data.iloc[0]['date']
            end_date = cum_data.iloc[-1]['date']
            date_range = list(datetime_range(start_date, end_date))
            cum_data.set_index('date', inplace=True)
            cum_data = cum_data.reindex(date_range)
            cum_data.ffill(inplace=True)
            cum_data.reset_index(inplace=True)

        daily_data = cum_data.copy()
        daily_data.iloc[1:, 1:] = daily_data.iloc[:, 1:].diff().iloc[1:]
        daily_data_melt = daily_data.melt(id_vars=['date'], var_name='district', value_name='daily_no')
        daily_data_melt.set_index(['date'], inplace=True)

        cum_data_melt = cum_data.melt(id_vars=['date'], var_name='district', value_name='cum_no')
        cum_data_melt.set_index(['date'], inplace=True)

        data = pd.concat([cum_data_melt, daily_data_melt[['daily_no']]], axis=1)
        data[['cum_no', 'daily_no']] = data[['cum_no', 'daily_no']].astype('int32')

        return data

    confirmed_by_dist_gp_timeline = get_cum_daily_by_distict('district_data/provincial_gp_cumulative.csv', province='gp')
    confirmed_by_dist_gp_timeline.to_csv("data/gp/confirmed_by_dist_gp_timeline.csv")

    print("GP Pre-Processing Completed")


def copy_data_local():
    src_path = "data"
    dest_path = "C:/Users/Simon/Documents/Additional/Data Science/Coronavirus_SA/Website/Covid19SAData_JS/test_data"
    shutil.rmtree(dest_path)  # BE CAREFUL WITH THIS
    shutil.copytree(src_path, dest_path)
    print("Data copied locally")


def preprocess_all():
    print("----------------------")
    print("Pre-Processing Started")
    preprocess_sa_data()
    preprocess_prov_data()
    # preprocess_gp_data()
    print("Pre-Processing Completed")


if __name__ == '__main__':
    preprocess_all()
    if sys.argv and sys.argv[0] == "pythonanywhere":
        pass
    else:
        copy_data_local()
