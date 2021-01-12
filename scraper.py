# from subprocess import call
from subprocess import Popen, PIPE
import pandas as pd
from datetime import datetime


def scrape_data():

    p = Popen(["node", "js_scraping/scraper.js"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output = p.stdout  # .read().decode("utf-8")

    def next_line():
        return output.readline().decode("utf-8").replace("\n", "")

    no_sources = 1  # TODO - Implement

    for i in range(no_sources):
        # Gen Data
        source = next_line()
        heading = next_line()
        data_date = next_line()
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

        input_date_format = '%d %b %Y'

        # ------------------
        #       CASES
        # ------------------
        cases_data_df = input_to_df(print_dict=True, headings=['Province', 'Cases'])

        cases_data_df_piv = transform_df(cases_data_df, 'Cases')

        add_date(cases_data_df_piv, data_date, input_date_format)
        cases_data_df_piv['source'] = source
        cases_date = cases_data_df_piv['date'].iloc[-1]  # need date in this format
        cases_data_df_piv.set_index('date', inplace=True)

        prov_cum_cases = pd.read_csv('data/scraped/covid19za_provincial_cumulative_timeline_confirmed.csv', index_col='date')

        # Append new day's data to csv if it has not already been added otherwise updated day's data
        # Update values instead of doing nothing in case values have been changed
        if cases_date in prov_cum_cases.index:
            prov_cum_cases.loc[cases_date] = cases_data_df_piv.loc[cases_date]
        else:
            prov_cum_cases = prov_cum_cases.append(cases_data_df_piv)

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

        # Append new day's data to csv if it has not already been added otherwise updated day's data
        # Update values instead of doing nothing in case values have been changed
        if cases_date in prov_cum_deaths.index:
            prov_cum_deaths.loc[deaths_date] = deaths_data_df_piv.loc[deaths_date]
        else:
            prov_cum_deaths = prov_cum_deaths.append(deaths_data_df_piv)

        prov_cum_deaths.to_csv("data/scraped/covid19za_provincial_cumulative_timeline_deaths.csv", index=True)

        # RECOVERIES
        add_date(recovered_data_df_piv, data_date, input_date_format)
        recovered_data_df_piv['source'] = source
        recovered_date = recovered_data_df_piv['date'].iloc[-1]  # need date in this format
        recovered_data_df_piv.set_index('date', inplace=True)

        prov_cum_recovered = pd.read_csv('data/scraped/covid19za_provincial_cumulative_timeline_recoveries.csv', index_col='date')

        # Append new day's data to csv if it has not already been added otherwise updated day's data
        # Update values instead of doing nothing in case values have been changed
        if recovered_date in prov_cum_recovered.index:
            prov_cum_recovered.loc[recovered_date] = recovered_data_df_piv.loc[recovered_date]
        else:
            prov_cum_recovered = prov_cum_recovered.append(recovered_data_df_piv)

        prov_cum_recovered.to_csv("data/scraped/covid19za_provincial_cumulative_timeline_recoveries.csv", index=True)

        # ------------------
        #     IS SUCCESS
        # ------------------
        is_success = next_line() == "SUCCESS"
        print(f"is_success: {is_success}")


if __name__ == "__main__":
    scrape_data()