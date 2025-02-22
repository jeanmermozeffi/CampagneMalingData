# -*- coding: utf-8 -*-

import requests
import os
from requests.exceptions import RequestException
from datetime import datetime, timedelta

from requests.packages.urllib3.exceptions import InsecureRequestWarning
import calendar

import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials


import pandas as pd


class SippecAPI:
    def __init__(self):
        self.header = {
            'header_brave_windows': {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'fr-FR,fr;q=0.6',
                'content-type': 'application/json',
                'sec-ch-ua': '\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Brave\";v=\"120\"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '\"Windows\"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'sec-gpc': '1',
                'referrer': 'https://sippecacademie.com/',
                'referrerPolicy': 'strict-origin-when-cross-origin',
            },
            'headers_chrome_mac': {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'fr-FR,fr;q=0.6',
                'content-type': 'application/json',
                'sec-ch-ua': '\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"99\", \"Chromium\";v=\"99\"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '\"Mac OS X\"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'sec-gpc': '1',
                'referrer': 'https://sippecacademie.com/',
                'referrerPolicy': 'strict-origin-when-cross-origin',
            }
        }

        self.CREDENTIALS_PATH = '/Users/jeanmermozeffi/DataspellProjects/scraping-master/credential/secret-reporting-sheet.json'
        self.current_date = datetime.now()
        self.current_month = self.current_date.month
        self.current_year = self.current_date.year
        self.base_url = 'https://sippecacademie.com'
        self.dates = self.previous_month_dates()

        self.gc = gc = self.get_credentials()

    def get_credentials(self):
        try:
            credentials = Credentials.from_service_account_file(self.CREDENTIALS_PATH, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
            gc = gspread.authorize(credentials)
            return gc
        except Exception as e:
            print(f"An error occurred while getting credentials: {str(e)}")
            return None

    @staticmethod
    def create_folder_if_not_exists(file_path, folder_path='Resultats'):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        return os.path.join(folder_path, file_path)

    @staticmethod
    def previous_month_dates():
        # Obtient la date actuelle
        date_today = datetime.now()
        # Obtient le premier jour du mois actuel
        first_day_current_month = date_today.replace(day=1)
        # Obtient le premier jour du mois précédent
        first_day_previous_month = first_day_current_month - timedelta(days=1)
        first_day_previous_month = first_day_previous_month.replace(day=1)
        # Obtient le dernier jour du mois précédent
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        last_day_previous_month = last_day_previous_month.replace(
            day=calendar.monthrange(last_day_previous_month.year, last_day_previous_month.month)[1])

        return first_day_previous_month.strftime("%d/%m/%Y"), last_day_previous_month.strftime("%d/%m/%Y")

    def get_month_ranges(self, year=None):
        list_start_date = []
        list_end_date = []

        limit_range = 13
        if year is None:
            year = self.current_date.year

        current_year = self.current_date.year

        if current_year == year:
            limit_range = self.current_date.month

        for month in range(1, limit_range):
            # Déterminer la date de début du mois
            start_of_month = datetime(year, month, 1)

            # Déterminer la date de fin du mois
            if month == 12:
                end_of_month = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_of_month = datetime(year, month + 1, 1) - timedelta(days=1)

            # Ajouter les dates de début et de fin à leur liste respective
            list_start_date.append(start_of_month.strftime("%d/%m/%Y"))
            list_end_date.append(end_of_month.strftime("%d/%m/%Y"))

        return list_start_date, list_end_date

    def get_previous_month_sheet(self, spreadsheet_name, sheet_name):
        # Déterminez le nom de la feuille précédant le mois actuel
        previous_month = self.current_month - 1 if self.current_month != 1 else 12
        previous_year = self.current_year if self.current_month != 1 else self.current_year - 1
        sheet_name = f"{sheet_name}_{previous_month:02d}-{previous_year}"

        # Ouvrir le classeur
        try:
            spreadsheet = self.gc.open(spreadsheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Le classeur '{spreadsheet_name}' n'a pas été trouvé.")
            return None

        # Récupérer la feuille précédant le mois actuel
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            return sheet
        except gspread.exceptions.WorksheetNotFound:
            print(f"La feuille '{sheet_name}' n'a pas été trouvée.")
            return None

    def get_previous_month_data(self, spreadsheet_name, sheet_name):
        # Récupérez la feuille précédant le mois actuel
        sheet = self.get_previous_month_sheet(spreadsheet_name, sheet_name)
        if sheet is None:
            return None

        # Récupérez les données de la feuille et convertissez-les en DataFrame
        try:
            data = sheet.get_all_values()
            headers = data.pop(0)
            df = pd.DataFrame(data, columns=headers)
            return df
        except Exception as e:
            print(f"Une erreur s'est produite lors de la récupération des données : {str(e)}")
            return None

    def get_data(self, url, date_from, date_end):
        pass

    def get_sippec_data(self, current_month=True, year=None):
        if year is None:
            year = self.current_date.year

        datas_dict = {}
        datas = []

        url = f"{self.base_url}:9000/user/getStatistique"
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        try:
            # Utiliser un seul objet session pour améliorer les performances des requêtes multiples
            with (requests.Session() as session):
                # Ignorer la vérification SSL pour éviter les erreurs liées aux certificats
                session.verify = False
                parameters = {
                    "user": 22,
                    "data": {
                        "paysCodeNumero": 225,
                    }
                }

                if current_month:
                    parameters['data']['dateDebut'], parameters['data']['dateFin'] = self.previous_month_dates()
                    response = session.post(url, json=parameters)
                    response.raise_for_status()
                    datas_dict = {
                        'data': response.json(),
                        'date': parameters['data']['dateDebut']
                    }
                    datas.append(datas_dict)

                    return datas

                list_start_date, list_end_date = self.get_month_ranges(year)

                for first_day_previous_month, last_day_previous_month in zip(list_start_date, list_end_date):
                    parameters['data']['dateDebut'] = first_day_previous_month
                    parameters['data']['dateFin'] = last_day_previous_month

                    response = session.post(url, json=parameters)
                    response.raise_for_status()
                    datas_dict = {
                        'data': response.json(),
                        'date': parameters['data']['dateDebut']
                    }
                    datas.append(datas_dict)

        except RequestException as e:
            print(f"An error occurred: {str(e)}")

        return datas

    @staticmethod
    def clean_dictionary(input_dict):
        keys_to_keep = []

        # Filtrer les clés à conserver (celles qui ne commencent pas par 'role')
        for key, value in input_dict.items():
            if not key.startswith('role'):
                # Si la valeur est une liste, la convertir en int
                if isinstance(value, list) and len(value) == 1:
                    value = int(value[0])
                keys_to_keep.append((key, value))

        # Créer un nouveau dictionnaire avec les clés filtrées
        cleaned_dict = dict(keys_to_keep)

        return cleaned_dict

    @staticmethod
    def explode_and_rename(df, columns_col, value_cols, index_col='date'):
        # Permet d'exploser la colonne 'columns_col' en plusieurs colonnes
        df_exploded = df.pivot_table(index=index_col, columns=columns_col, values=value_cols)

        # Renommer les colonnes en ajoutant le nom de la catégorie en préfixe
        df_exploded.columns = [f"{col[1]}_{col[0]}" for col in df_exploded.columns]
        df_exploded.reset_index(inplace=True)

        return df_exploded

    @staticmethod
    def get_scan_growth_rate(data_current, data_previous, column):
        # Vérifiez si les données sont numériques et convertissez-les si nécessaire
        if not pd.api.types.is_numeric_dtype(data_current[column]) or not pd.api.types.is_numeric_dtype(data_previous[column]):
            try:
                data_current[column] = pd.to_numeric(data_current[column])
                data_previous[column] = pd.to_numeric(data_previous[column])
            except ValueError:
                print("Les données dans la colonne ne peuvent pas être converties en numériques.")
                return None

        # Calcule la croissance du nombre de scans entre les données actuelles et précédentes
        sum_previous = data_previous[column].sum()
        if sum_previous == 0:
            print("La somme des valeurs dans la colonne précédente est zéro. Impossible de calculer le taux de croissance.")
            return None

        # Calcule la croissance du nombre de scans entre les données actuelles et précédentes
        growth_rate = ((data_current[column].sum() - sum_previous) / sum_previous) * 100
        growth_rate = round(growth_rate, 2)  # Arrondir à deux décimales

        # Crée un DataFrame avec le taux de croissance calculé
        scan_growth_rate = pd.DataFrame([f"{growth_rate}%"], index=[column], columns=["Taux de croissance"])

        return scan_growth_rate

    @staticmethod
    def get_multiple_scan_growth_rates(data_current, data_previous, columns):
        growth_rates = {}

        for column in columns:
            # Vérifiez si les données sont numériques et convertissez-les si nécessaire
            if not pd.api.types.is_numeric_dtype(data_current[column]) or not pd.api.types.is_numeric_dtype(data_previous[column]):
                try:
                    data_current[column] = pd.to_numeric(data_current[column])
                    data_previous[column] = pd.to_numeric(data_previous[column])
                except ValueError:
                    print(f"Les données dans la colonne {column} ne peuvent pas être converties en numériques.")
                    continue

            # Calcule la croissance du nombre de scans entre les données actuelles et précédentes
            sum_previous = data_previous[column].sum()
            if sum_previous == 0:
                print("La somme des valeurs dans la colonne précédente est zéro. Impossible de calculer le taux de croissance.")
                growth_rates[column] = None
                continue

            # Calcule la croissance du nombre de scans entre les données actuelles et précédentes
            growth_rate = ((data_current[column].sum() - data_previous[column].sum()) / data_previous[column].sum()) * 100
            growth_rate = round(growth_rate, 2)  # Arrondir à deux décimales

            # Stocke le taux de croissance calculé dans un dictionnaire
            growth_rates[column] = f"{growth_rate}%"

        return growth_rates

    def process_datas_operators(self, sippec_data, keys_to_keep):
        cleaned_data_operator = []
        for processed_data in sippec_data:
            datas = processed_data.get('data')['items']
            dates = processed_data.get('date')
            if datas:
                for index, data_item in enumerate(datas):
                    datas_operator = data_item.get(keys_to_keep)
                    if datas_operator:
                        for operator in datas_operator:
                            operator['date'] = dates
                            cleaned_data_operator.append(self.clean_dictionary(operator))
                    else:
                        print("No 'datasOperateur' found in the item.")
            else:
                print("No 'items' found in the data.")
        return cleaned_data_operator

    def get_academician_data(self):
        datas = []
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        headers = self.header['headers_chrome_mac']

        parameters = {
            "user": 22,
            "data": {
                "paysCodeNumero": 225
            }
        }
        try:
            # Utiliser un seul objet session pour améliorer les performances des requêtes multiples
            with (requests.Session() as session):
                # Ignorer la vérification SSL pour éviter les erreurs liées aux certificats
                session.verify = False
                url = f"{self.base_url}:9000/user/academicienParEtat"
                response = requests.post(url=url, headers=headers, json=parameters, verify=False)
                response.raise_for_status()

                academician_data = response.json()
                data_items = academician_data.get('items')
                if data_items is not None:
                    for count in range(len(data_items)):
                        cleaned_data = self.clean_dictionary(data_items[count])
                        cleaned_data['date'] = self.dates[0]
                        datas.append(cleaned_data)

        except RequestException as e:
            print(f"An error occurred: {str(e)}")

        return datas

    def get_academic_ranking_response(self, option, current_date=True):
        # Ignorer les avertissements SSL (uniquement pour cet exemple)
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

        url = f"{self.base_url}:9000/academicien/classementAcademicien"
        parameters = {
            "user": 22,
            "size": "5",
            "index": 0,
            "data": {
                "paysCodeNumero": "225",
                "optionClassement": option,
                "groupeId": None
            }
        }

        if current_date:
            first_day_previous_month, last_day_previous_month = self.previous_month_dates()
            parameters['data']['dateDebut'] = first_day_previous_month
            parameters['data']['dateFin'] = last_day_previous_month

        try:
            response = requests.post(url, json=parameters, verify=False)
            if response.status_code == 200:
                data_response = {
                    "data": response.json(),
                    "date": parameters['data']['dateDebut']
                }

                return data_response

            else:
                print(f"Erreur lors de la requête. Code de statut : {response.status_code}")
                return None
        except Exception as e:
            print(f"Une erreur s'est produite : {str(e)}")
            return None

    def get_academic_ranking(self, option):
        rankings_response = self.get_academic_ranking_response(option)
        rankings = rankings_response['data']
        date = rankings_response['date']

        data_rankings = []

        if rankings:
            for ranking in rankings.get('items'):
                rankings_dict = {
                    'id': ranking.get('id'),
                    'matricule': ranking.get('matricule'),
                    'nom': ranking.get('nom')
                }

                datas_libelle = ranking.get('datasLibelleCategorie')
                datas_somme_scan = ranking.get('datasSommeScanCategorie')
                datas_total_scan = ranking.get('datasTotalScanCategorie')

                for libelle, somme_scan, total_scan in zip(datas_libelle, datas_somme_scan, datas_total_scan):
                    rankings_dict[f'{libelle}_SOMME_SCAN'] = somme_scan
                    rankings_dict[f'{libelle}_TOTAL_SCAN'] = total_scan
                    rankings_dict['date_ranking'] = date

                data_rankings.append(rankings_dict)

        return data_rankings, date

    def get_data_for_month(self, spreadsheet_name, sheet_name, year, month):
        # Déterminez le nom de la feuille correspondant au mois et à l'année spécifiés
        # sheet_name = f"{sheet_name}_{month:02d}-{year}"

        # Ouvrir le classeur
        try:
            spreadsheet = self.gc.open(spreadsheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Le classeur '{spreadsheet_name}' n'a pas été trouvé.")
            return None

        # Récupérer les données de la feuille
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            data = sheet.get_all_records()
        except gspread.exceptions.WorksheetNotFound:
            print(f"La feuille '{sheet_name}' n'a pas été trouvée.")
            return None

        # Convertir les données en DataFrame
        df = pd.DataFrame(data)

        # Convertir la colonne 'date' au format datetime
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

        # Filtrer les données pour le mois spécifié
        df_months = df[(df['date'].dt.year == year) & (df['date'].dt.month == month)]

        return df_months

    def create_or_clear_worksheet(self, spreadsheet_name, sheet_name, dataframe, df_row=None, col_row=1):
        include_header = True
        month_date = datetime.strptime(self.dates[0], '%d/%m/%Y')
        _SHEET_NAME = f"{sheet_name} {month_date.month}_{month_date.year}"

        if sheet_name == 'SIPPEC Academie Reporting Data ALL':
            _SHEET_NAME = sheet_name

        try:
            spreadsheet = self.gc.open(spreadsheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            spreadsheet = self.gc.create(spreadsheet_name)

        try:
            sheet = spreadsheet.worksheet(_SHEET_NAME)
            all_values = sheet.get_all_values()
            if df_row is None:
                if len(all_values) > 0:
                    # Si df_row n'est pas spécifié, déterminez la première ligne vide dans la feuille
                    df_row = len(all_values) + 1
                    include_header = False
                else:
                    df_row = 1

        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=_SHEET_NAME, rows=70, cols=30)
            df_row = 1

        set_with_dataframe(sheet, dataframe, row=df_row, col=col_row, include_index=False, include_column_header=include_header)


if __name__ == "__main__":
    sippec = SippecAPI()
    spreadsheet_name = "Reporting SIPPEC Academie Data"
    sheet_name = "Report Sippec_04-2024"
    data = sippec.get_previous_month_data(spreadsheet_name, sheet_name)
    print(data)



