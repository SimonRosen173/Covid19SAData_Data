# from subprocess import call
from subprocess import Popen, PIPE
import pandas as pd
from datetime import datetime
import io
import requests


def scrape_data(no_sources=1):
    p = Popen(["node", "js_scraping/scraper.js", str(no_sources)], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output = p.stdout  # .read().decode("utf-8")

    def next_line():
        return output.readline().decode("utf-8").replace("\n", "")

    no_sources = int(next_line())  # should be same as arg of this method

    for i in range(no_sources):
        input_date_format = '%d %b %Y'
        stored_date_format = '%d-%m-%Y'

        # Gen Data
        source = next_line()
        heading = next_line()
        data_date = next_line()
        date_datetime = datetime.strptime(data_date, input_date_format)
        next_line()

        print(f"{source} {heading} {data_date}")

        # def input_to_df():

        def input_to_df(print_dict=True, headings=[]):
            if headings:  # if headings not empty - ignore input headings
                curr_headings = headings
                next_line()
            else:
                curr_headings = next_line().split(",")

            curr_data = []  # [[]]*11
            while True:
                curr_line = next_line()
                if curr_line != "###":
                    curr_data.append(curr_line.split(","))
                else:
                    break

            tmp_data = list(zip(*curr_data))
            transp_data = [list(tmp_data[i]) for i in range(len(tmp_data))]
            curr_data_dict = {key: val for key, val in zip(curr_headings, transp_data)}

            if print_dict:
                print(curr_data_dict)

            curr_data_df = pd.DataFrame.from_dict(curr_data_dict)

            return curr_data_df

        prov_map = {
            "Eastern Cape": "EC",
            "Free State": "FS",
            "Gauteng": "GP",
            "KwaZulu-Natal": "KZN",
            "Limpopo": "LP",
            "Mpumalanga": "MP",
            "North West": "NW",
            "Northern Cape": "NC",
            "Western Cape": "WC",
            "Unknown": "UNKNOWN",
            "Total": "total"
        }

        # Going to assume columns will be 'Province'
        def transform_df(df, value_name):
            tmp_df = df[['Province', value_name]].copy()
            tmp_df['Province'] = tmp_df['Province'].map(prov_map)
            tmp_df[value_name] = pd.to_numeric(tmp_df[value_name]).round()
            tmp_df['i'] = 0

            tmp_df_piv = tmp_df.pivot(index='i', columns='Province', values=[value_name])
            tmp_df_piv.index.name = None
            tmp_df_piv = tmp_df_piv.droplevel(0, axis=1)
            tmp_df_piv.columns.name = None

            return tmp_df_piv
            # return out_df

        # Note this is done by reference
        def add_date(df, given_date, date_format):
            df['date'] = datetime.strptime(given_date, date_format).strftime('%d-%m-%Y')  # '09-01-2021'
            df['YYYYMMDD'] = datetime.strptime(given_date, date_format).strftime('%Y%m%d')  # '20210109'

        def sort_by_date(df: pd.DataFrame, date_format: str):
            df.reset_index(inplace=True)
            df['date'] = pd.to_datetime(df['date'], format=date_format)
            df.sort_index(inplace=True)
            df['date'] = df['date'].apply(lambda x: x.strftime(date_format))  # datetime.strptime(given_date, date_format).strftime('%d-%m-%Y')
            df.set_index('date', inplace=True)

        # ------------------
        #       CASES
        # ------------------
        cases_data_df = input_to_df(print_dict=True, headings=['Province', 'Cases'])

        cases_data_df_piv = transform_df(cases_data_df, 'Cases')

        add_date(cases_data_df_piv, data_date, input_date_format)
        cases_data_df_piv['source'] = source
        cases_date = cases_data_df_piv['date'].iloc[-1]  # need date in this format
        cases_data_df_piv.set_index('date', inplace=True)
        # cases_data_df_piv.sort_index(inplace=True)

        prov_cum_cases = pd.read_csv('data/scraped/covid19za_provincial_cumulative_timeline_confirmed.csv', index_col='date')

        # Append new day's data to csv if it has not already been added otherwise updated day's data
        # Update values instead of doing nothing in case values have been changed
        if cases_date in prov_cum_cases.index:
            prov_cum_cases.loc[cases_date] = cases_data_df_piv.loc[cases_date]
        else:
            prov_cum_cases = prov_cum_cases.append(cases_data_df_piv)

        sort_by_date(prov_cum_cases, stored_date_format)
        prov_cum_cases.to_csv("data/scraped/covid19za_provincial_cumulative_timeline_confirmed.csv", index=True)

        # ------------------
        #       TESTS
        # ------------------
        tests_data_df = input_to_df(print_dict=True, headings=['Sector', 'Tests'])

        sector_map = {
            "PRIVATE": "cumulative_tests_private",
            "PUBLIC": "cumulative_tests_public",
            "Total": "cumulative_tests"
        }

        tests_df = tests_data_df[['Sector', 'Tests']].copy()
        tests_df['Sector'] = tests_df['Sector'].map(sector_map)
        tests_df['Tests'] = pd.to_numeric(tests_df['Tests']).round()
        tests_df['i'] = 0

        tests_df_piv = tests_df.pivot(index='i', columns='Sector', values=['Tests'])
        tests_df_piv.index.name = None
        tests_df_piv = tests_df_piv.droplevel(0, axis=1)
        tests_df_piv.columns.name = None

        add_date(tests_df_piv, data_date, input_date_format)
        tests_df_piv['source'] = source
        tests_date = tests_df_piv['date'].iloc[-1]
        tests_df_piv.set_index('date', inplace=True)

        cum_tests = pd.read_csv('data/scraped/covid19za_timeline_testing.csv', index_col='date')

        # Append new day's data to csv if it has not already been added otherwise updated day's data
        # Update values instead of doing nothing in case values have been changed
        if tests_date in cum_tests.index:
            cum_tests.loc[tests_date] = tests_df_piv.loc[tests_date]
        else:
            cum_tests = cum_tests.append(tests_df_piv)

        # cum_tests.sort_index(inplace=True, ascending=False)
        sort_by_date(cum_tests, stored_date_format)
        cum_tests.to_csv('data/scraped/covid19za_timeline_testing.csv', index=True)

        # ---------------------
        #  DEATHS & RECOVERIES
        # ---------------------
        deaths_recovered_data_df = input_to_df(print_dict=True, headings=['Province', 'Deaths', 'Recoveries'])

        deaths_data_df_piv = transform_df(deaths_recovered_data_df, 'Deaths')
        recovered_data_df_piv = transform_df(deaths_recovered_data_df, 'Recoveries')

        # DEATHS
        add_date(deaths_data_df_piv, data_date, input_date_format)
        deaths_data_df_piv['source'] = source
        deaths_date = deaths_data_df_piv['date'].iloc[-1]  # need date in this format
        deaths_data_df_piv.set_index('date', inplace=True)

        prov_cum_deaths = pd.read_csv('data/scraped/covid19za_provincial_cumulative_timeline_deaths.csv', index_col='date')
        # prov_cum_deaths['date'] = pd.to_datetime(prov_cum_deaths['date'], format=stored_date_format)
        # prov_cum_deaths.set_index('date', inplace=True)

        # Append new day's data to csv if it has not already been added otherwise updated day's data
        # Update values instead of doing nothing in case values have been changed
        if cases_date in prov_cum_deaths.index:
            prov_cum_deaths.loc[deaths_date] = deaths_data_df_piv.loc[deaths_date]
        else:
            prov_cum_deaths = prov_cum_deaths.append(deaths_data_df_piv)

        # prov_cum_deaths.sort_index(inplace=True, ascending=False)
        sort_by_date(prov_cum_deaths, stored_date_format)
        prov_cum_deaths.to_csv("data/scraped/covid19za_provincial_cumulative_timeline_deaths.csv", index=True)

        # RECOVERIES
        add_date(recovered_data_df_piv, data_date, input_date_format)
        recovered_data_df_piv['source'] = source
        recovered_date = recovered_data_df_piv['date'].iloc[-1]  # need date in this format
        recovered_data_df_piv.set_index('date', inplace=True)

        prov_cum_recovered = pd.read_csv('data/scraped/covid19za_provincial_cumulative_timeline_recoveries.csv', index_col='date')
        # prov_cum_recovered['date'] = pd.to_datetime(prov_cum_recovered['date'], format=stored_date_format)
        # prov_cum_recovered.set_index('date', inplace=True)

        # Append new day's data to csv if it has not already been added otherwise updated day's data
        # Update values instead of doing nothing in case values have been changed
        if recovered_date in prov_cum_recovered.index:
            prov_cum_recovered.loc[recovered_date] = recovered_data_df_piv.loc[recovered_date]
        else:
            prov_cum_recovered = prov_cum_recovered.append(recovered_data_df_piv)

        # prov_cum_recovered.sort_index(inplace=True, ascending=False)
        sort_by_date(prov_cum_recovered, stored_date_format)
        prov_cum_recovered.to_csv("data/scraped/covid19za_provincial_cumulative_timeline_recoveries.csv", index=True)

        # ------------------
        #     IS SUCCESS
        # ------------------
        is_success = next_line() == "SUCCESS"
        print(f"is_success: {is_success}")


def set_data_from_repo():
    # get dataframe from specified url using kwargs specified for read_csv
    def df_from_url(df_url, pd_kwargs={}, use_base_url=True) -> pd.DataFrame:
        base_url = "https://raw.githubusercontent.com/SimonRosen173/Covid19SAData_Data/master/data/scraped/"
        if use_base_url:
            df_url = base_url + df_url
        df_req = requests.get(df_url).content
        df = pd.read_csv(io.StringIO(df_req.decode('utf-8')), **pd_kwargs)
        return df

    local_path = "data/scraped/"
    # Cases
    cases_df = df_from_url('covid19za_provincial_cumulative_timeline_confirmed.csv', {"index_col": "date"})
    cases_df.to_csv(local_path+"covid19za_provincial_cumulative_timeline_confirmed.csv")
    # Tests
    tests_df = df_from_url('covid19za_timeline_testing.csv', {"index_col": "date"})
    tests_df.to_csv(local_path + "covid19za_timeline_testing.csv")
    # Recoveries
    recoveries_df = df_from_url('covid19za_provincial_cumulative_timeline_recoveries.csv', {"index_col": "date"})
    recoveries_df.to_csv(local_path + "covid19za_provincial_cumulative_timeline_recoveries.csv")
    # Deaths
    deaths_df = df_from_url('covid19za_provincial_cumulative_timeline_deaths.csv', {"index_col": "date"})
    deaths_df.to_csv(local_path + "covid19za_provincial_cumulative_timeline_deaths.csv")


if __name__ == "__main__":
    scrape_data(1)
    # set_data_from_repo()
