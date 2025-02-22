import os
from datetime import datetime

import pandas as pd

from credential.secrets import sippec, path
from modules.spreadsheets.spreadsheets import GoogleSheetsService
from modules.vbout.vboutapi import VboutApi


previous_month_sheet_name = 'Rapport_Mailing_Mensuel_SIPPEC_7_2024'
SPREADSHEET_NAME = sippec.get('SPREADSHEET_NAME')
sheet_service = GoogleSheetsService(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'), spreadsheet_name=SPREADSHEET_NAME)
vbout_api = VboutApi()


if __name__ == '__main__':
    # first_day_previous_month, last_day_previous_month = vbout_api.previous_month_dates()
    # date_previous_obj = datetime.strptime(first_day_previous_month, "%m/%d/%Y").date()
    # current_month_sheet_name = f'Rapport_Mailing_Mensuel_SIPPEC_{date_previous_obj.month}_{date_previous_obj.year}'
    # previous_month_sheet_name = (f'Rapport_Mailing_Mensuel_SIPPEC_{int(date_previous_obj.month)-1}_'
    #                              f'{date_previous_obj.year}')
    #
    # data_current_date = sheet_service.get_data(sheet_name=current_month_sheet_name, start_cell='D', end_cell='O')
    # data_previous_date = sheet_service.get_data(sheet_name=previous_month_sheet_name, start_cell='D', end_cell='O')
    # if data_previous_date:
    #     print(data_previous_date)
    # else:
    #     print("No data found.")
    year = datetime.now().year
    previous_year = datetime.now().year - 1
    print(f'YEAR: {year}')
    print(f'PREVIOUS YEAR: {previous_year}')

    # data_previous_year = sheet_service.get_data_previous_year(2023)
    # data_current_year = get_data_previous_year(sheet_service, 2024)



