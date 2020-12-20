import pandas as pd
import numpy as np
import io
import requests
from datetime import timedelta, datetime


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


# Data at country level
def preprocess_sa_data():
    def get_cum_daily(data_url, cum_col='total', index_col='date'):  # kwargs={}):
        cols = ['date', 'total']
        pd_kwargs = {"usecols": [cum_col, index_col], "index_col": [index_col]}

        data = df_from_url(data_url, pd_kwargs)
        data.reset_index(inplace=True)
        data['date'] = pd.to_datetime(data['date'], format='%d-%m-%Y')
        data.set_index('date', inplace=True)
        data.rename({cum_col: "cum_no"}, axis=1, inplace=True)
        data.ffill(inplace=True)

        data['daily_no'] = data['cum_no']
        data['daily_no'][1:] = data['cum_no'].diff()[1:]
        # Cast columns to integer
        data = data.astype('int32')
        return data

    confirmed_cases_url = "covid19za_provincial_cumulative_timeline_confirmed.csv"
    confirmed_data = get_cum_daily(confirmed_cases_url)

    deaths_url = "covid19za_provincial_cumulative_timeline_deaths.csv"
    deaths_data = get_cum_daily(deaths_url)

    tests_url = "covid19za_timeline_testing.csv"
    tests_data = get_cum_daily(tests_url, 'cumulative_tests', 'date')

    recovered_data = get_cum_daily(tests_url, 'recovered', 'date')

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
        _active_data = _active_data.astype('int32')

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
        _all_cum_data.ffill(inplace=True)
        _all_cum_data.fillna(0, inplace=True)
        _all_cum_data = _all_cum_data.astype('int32')

        # DERIVED STATS

        # confirmed_div_by_tests
        _all_cum_data['confirmed_div_by_tests'] = _all_cum_data['confirmed'] / _all_cum_data['tests']
        _all_cum_data['confirmed_div_by_tests'] = _all_cum_data['confirmed_div_by_tests'].round(3)

        # deaths_div_by_confirmed
        _all_cum_data['deaths_div_by_confirmed'] = _all_cum_data['deaths'] / _all_cum_data['confirmed']
        _all_cum_data['deaths_div_by_confirmed'] = _all_cum_data['deaths_div_by_confirmed'].round(3)
        _all_cum_data.fillna(0.000, inplace=True)

        # recovered_div_by_confirmed
        _all_cum_data['recovered_div_by_confirmed'] = _all_cum_data['recovered'] / _all_cum_data['confirmed']
        _all_cum_data['recovered_div_by_confirmed'] = _all_cum_data['recovered_div_by_confirmed'].round(3)
        _all_cum_data.fillna(0.000, inplace=True)

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
        _all_cum_data.fillna(0.00, inplace=True)

        return _all_cum_data

    # All cumulative data
    all_cum_data = get_all_cum_data()
    all_cum_data.to_csv('data/all_cum_data.csv')

    def get_all_daily_data():
        _all_daily_data = confirmed_data[['daily_no']].rename({"daily_no": "confirmed"}, axis=1)
        _all_daily_data = pd.concat([
            _all_daily_data,
            tests_data[['daily_no']].rename({"daily_no": "tests"}, axis=1),
            deaths_data[['daily_no']].rename({"daily_no": "deaths"}, axis=1),
            recovered_data[['daily_no']].rename({"daily_no": "recovered"}, axis=1),
            active_data[['daily_no']].rename({"daily_no": "active"}, axis=1),

        ], axis=1)
        _all_daily_data.ffill(inplace=True)
        _all_daily_data.fillna(0, inplace=True)
        _all_daily_data = _all_daily_data.astype('int32')
        return _all_daily_data

    # All daily data
    all_daily_data = get_all_daily_data()
    all_daily_data.to_csv("data/all_daily_data.csv")

    def get_index_page_data():
        def zero_space(num):
            return format(num, ',d').replace(",", " ")

        def format_date(date: datetime) -> str:
            return date.strftime("%d/%m/%Y")

        # Tests
        tot_tested = zero_space(tests_data.iloc[-1]['cum_no'].astype(int))
        change_tested = zero_space(tests_data.iloc[-1]['daily_no'].astype(int))
        # tmp = tests_data.reset_index()['date'].tail(1)
        last_date_tested = format_date(tests_data.index[-1])
        second_last_date_tested = format_date(tests_data.index[-2])

        # Confirmed
        tot_confirmed = zero_space(confirmed_data.iloc[-1]['cum_no'].astype(int))
        change_confirmed = zero_space(confirmed_data.iloc[-1]['daily_no'].astype(int))
        last_date_confirmed = format_date(confirmed_data.index[-1])
        second_last_date_confirmed = format_date(confirmed_data.index[-2])

        # Active
        tot_active = zero_space(active_data.iloc[-1]['cum_no'].astype(int))
        change_active = zero_space(active_data.iloc[-1]['daily_no'].astype(int))
        last_date_active = format_date(confirmed_data.index[-1])
        second_last_date_active = format_date(confirmed_data.index[-2])

        # Deaths
        tot_deaths = zero_space(deaths_data.iloc[-1]['cum_no'].astype(int))
        change_deaths = zero_space(deaths_data.iloc[-1]['daily_no'].astype(int))
        last_date_deaths = format_date(deaths_data.index[-1])
        second_last_date_deaths = format_date(deaths_data.index[-2])

        # Recoveries
        tot_recoveries = zero_space(recovered_data.iloc[-1]['cum_no'].astype(int))
        change_recoveries = zero_space(recovered_data.iloc[-1]['daily_no'].astype(int))
        last_date_recoveries = format_date(recovered_data.index[-1])
        second_last_date_recoveries = format_date(recovered_data.index[-2])

        now = datetime.now()
        current_time = now.strftime("%H:%M %d %B %Y")

        _gen_data = pd.DataFrame(dict(
            tot_confirmed=[tot_confirmed], change_confirmed=[change_confirmed], last_date_confirmed=[last_date_confirmed],
            second_last_date_confirmed=[second_last_date_confirmed],

            tot_deaths=[tot_deaths], change_deaths=[change_deaths],  last_date_deaths=[last_date_deaths],
            second_last_date_deaths=[second_last_date_deaths],

            tot_active=[tot_active], change_active=[change_active], last_date_active=[last_date_active],
            second_last_date_active=[second_last_date_active],

            tot_tests=[tot_tested], change_tests=[change_tested], last_date_tests=[last_date_tested],
            second_last_date_tests=[second_last_date_tested],

            tot_recoveries=[tot_recoveries], change_recoveries=[change_recoveries], last_date_recoveries=[last_date_recoveries],
            second_last_date_recoveries=[second_last_date_recoveries],

            processed_datetime=[current_time]))

        return _gen_data

    index_page_data = get_index_page_data()
    index_page_data.to_csv("data/gen_data.csv", index=False)


if __name__ == '__main__':
    preprocess_sa_data()
