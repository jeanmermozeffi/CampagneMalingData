import os
import pandas as pd

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound

from typing import List, Optional, Dict, Any


class GoogleSheetsService:
    def __init__(self, credentials_file, spreadsheet_name):
        self.credentials_file = credentials_file
        self.spreadsheet_name = spreadsheet_name
        self.gc = None
        self.spreadsheet = None
        self.connect()
        self.current_sheet_name = None
        self.client = None
        self.connect_to_sheet()

    def connect(self):
        try:
            self.gc = gspread.service_account(filename=self.credentials_file)
            self.spreadsheet = self.gc.open(self.spreadsheet_name)
            print(f"Connecté à la feuille de calcul : {self.spreadsheet_name}")
        except Exception as e:
            print(f"Erreur lors de la connexion à la feuille de calcul: {e}")

    def connect_to_sheet(self):
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open(self.spreadsheet_name)
        except Exception as e:
            print(f"Error connecting to Google Sheets: {e}")
            raise

    @staticmethod
    def bold_headers(self, sheet, start_cell="A1"):
        try:
            num_columns = len(sheet.row_values(1))
            header_range = f"{start_cell}:{chr(ord(start_cell[0]) + num_columns - 1)}1"
            bold_format = {
                "textFormat": {"bold": True}
            }
            sheet.format(header_range, bold_format)
        except Exception as e:
            print(f"Erreur lors de l'application du format en gras aux en-têtes: {e}")

    @staticmethod
    def convert_datetime_columns_to_string(dataframe):
        for column in dataframe.columns:
            if pd.api.types.is_datetime64_any_dtype(dataframe[column]):
                dataframe[column] = dataframe[column].dt.strftime('%Y-%m-%d %H:%M:%S')

    def create_sheet(self, sheet_name):
        try:
            new_sheet = self.spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            print(f"Feuille '{sheet_name}' créée avec succès.")
            return new_sheet
        except gspread.exceptions.APIError as e:
            if 'A sheet with the name' in str(e) and 'already exists' in str(e):
                print(f"La feuille '{sheet_name}' existe déjà. Mise à jour en cours.")
                existing_sheet = self.get_sheet(sheet_name)
                if existing_sheet:
                    return existing_sheet
                else:
                    print(f"Erreur lors de la récupération de la feuille existante: {e}")
                    return None
            else:
                print(f"Erreur lors de la création de la feuille: {e}")
                return None

    def get_sheet(self, sheet_name):
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Feuille '{sheet_name}' introuvable.")
            return None

    def get_data(self, sheet_name: str, start_cell: Optional[str] = None, end_cell: Optional[str] = None,
                 expected_headers: Optional[List[str]] = None) -> Optional[List[Dict[str, Any]]]:
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)

            if start_cell and end_cell:
                cell_range = f"{start_cell}:{end_cell}"
                data = worksheet.get(cell_range)
            else:
                data = worksheet.get_all_values()

            # Convertir les données au format dictionnaire
            if data:
                if expected_headers:
                    headers = data[0]
                    if not all(header in headers for header in expected_headers):
                        print(f"Headers in the sheet do not match the expected headers.")
                        return None

                    # Réorganiser les lignes en fonction des en-têtes attendus
                    return [{header: row[i] if i < len(row) else None for i, header in enumerate(expected_headers)}
                            for row in data[1:]]

                # Convertir en liste de dictionnaires avec les en-têtes comme clés
                return [dict(zip(data[0], row)) for row in data[1:]]

            return None

        except WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found in the spreadsheet '{self.spreadsheet_name}'.")
            return None
        except SpreadsheetNotFound:
            print(f"Spreadsheet '{self.spreadsheet_name}' not found.")
            return None
        except Exception as e:
            print(f"An error occurred while retrieving data: {e}")
            return None

    def get_data_previous_year(self, year: int) -> dict[str, list[Any] | list[str | Any]]:
        data = {
            'months': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            'delivery_success': [],
            'delivery_failed': [],
            'open': [],
            'unopened': [],
            'bounced': [],
            'unsubscribed': [],
            'total_clicks': [],
            'total_clicks_unique': []
        }

        for month in range(1, 13):
            sheet_name = f'Rapport_Mailing_Mensuel_SIPPEC_{month}_{year}'
            try:
                month_data = self.get_data(sheet_name)
                if month_data:
                    month_data = month_data[0]
                    data['delivery_success'].append(int(month_data['delivery_success']))
                    data['delivery_failed'].append(int(month_data['delivery_failed']))
                    data['open'].append(int(month_data['open']))
                    data['unopened'].append(int(month_data['unopened']))
                    data['bounced'].append(int(month_data['bounced']))
                    data['unsubscribed'].append(int(month_data['unsubscribed']))
                    data['total_clicks'].append(int(month_data['total_clicks']))
                    data['total_clicks_unique'].append(int(month_data['total_clicks_unique']))
                else:
                    raise ValueError("No data found")
            except Exception as e:
                print(f"Error: {e}")
                # st.warning(f"Could not retrieve data for {sheet_name}: {e}")
                data['delivery_success'].append(0)
                data['delivery_failed'].append(0)
                data['open'].append(0)
                data['unopened'].append(0)
                data['bounced'].append(0)
                data['unsubscribed'].append(0)
                data['total_clicks'].append(0)
                data['total_clicks_unique'].append(0)

        return data

    def write_data(self, sheet_name, data, start_cell="A1"):
        sheet = self.get_sheet(sheet_name)

        if not sheet:
            new_sheet = self.create_sheet(sheet_name)
            new_sheet.update(range_name=start_cell, values=data)

        if sheet:
            try:
                existing_data = sheet.get_all_values()
                if existing_data:
                    self.update_sheet(sheet, data, start_cell)

                print("Données écrites avec succès.")
            except Exception as e:
                print(f"Erreur lors de l'écriture des données dans la feuille: {e}")

    @staticmethod
    def update_sheet(existing, new_data, range_name):
        try:
            existing_data = existing.get_all_values()
            if not existing_data:
                existing.append_row(new_data[0])
                print(f"Feuille '{existing.title}' mise à jour avec succès.")
            else:
                existing.update(range_name=range_name, values=new_data)
                print(f"Feuille '{existing.title}' mise à jour avec succès.")
        except Exception as e:
            print(f"Erreur lors de la mise à jour de la feuille: {e}")

    def create_dataframe(self, data):
        df = pd.DataFrame(data)

        # Convertissez les colonnes appropriées en types de données numériques
        numeric_columns = ['delivery_success', 'delivery_failed', 'unsubscribed', 'open', 'unopened', 'bounced',
                           'total_clicks', 'total_clicks_unique']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        self.convert_datetime_columns_to_string(df)
        df['total_deliveries'] = df[['delivery_success', 'delivery_failed']].sum(axis=1)

        return df

    def write_dataframe(self, sheet_name, dataframe, start_cell="A1"):
        sheet = self.get_sheet(sheet_name)

        if sheet:
            try:
                data = [dataframe.columns.tolist()] + dataframe.values.tolist()
                sheet.update(range_name=start_cell, values=data)
                print("DataFrame écrit avec succès dans la feuille de calcul.")
            except Exception as e:
                print(f"Erreur lors de l'écriture du DataFrame dans la feuille de calcul: {e}")

    @staticmethod
    def get_dataframe_pivot(aggregated_df):
        champs = ["open", "unopened", "bounced", "unsubscribed"]
        col_stats = ['Ouvert', 'Non ouvert', 'Rebondi', 'Désabonné']
        column_mapping = dict(zip(champs, col_stats))
        _df = aggregated_df.rename(columns=column_mapping)

        # Utiliser melt() pour transformer les colonnes en deux colonnes
        melted_df = _df.melt(value_vars=col_stats, var_name="champ", value_name="valeur")
        pivot_df = melted_df.set_index("champ")

        return pivot_df

    def write_dataframe_pivot(self, aggregated_df, start_cell="A2"):
        pivot_df = self.get_dataframe_pivot(aggregated_df)

        sheet = self.get_sheet(self.current_sheet_name)
        if sheet:
            try:
                # Convertir le DataFrame en une liste de listes pour l'écriture dans la feuille de calcul
                data = pivot_df.reset_index().values.tolist()
                # Mettre à jour la plage de cellules avec les données du DataFrame
                sheet.update(range_name=start_cell, values=data)

                print("DataFrame écrit avec succès dans la feuille de calcul.")
            except Exception as e:
                print(f"Erreur lors de l'écriture du DataFrame dans la feuille de calcul: {e}")

    @staticmethod
    def aggregated_stats_by_month(dataframe, aggregation_fields=None):
        if aggregation_fields is None:
            aggregation_fields = ['delivery_success', 'delivery_failed', 'unsubscribed', 'open', 'unopened', 'bounced',
                                  'total_clicks', 'total_clicks_unique', 'total_deliveries']

        aggregation_dict = {field: 'sum' for field in aggregation_fields}
        aggregated_df = dataframe.groupby(['year', 'month']).agg(aggregation_dict).reset_index()
        aggregated_df = aggregated_df.sort_values(by=['year', 'month'], ascending=False)
        aggregated_df['from_name'] = dataframe['from_name'][0]

        return aggregated_df

    def write_aggregated_stats_by_month_to_sheets(self, aggregated_df):
        for _, month_data in aggregated_df.iterrows():
            year_str = month_data['year']
            month_str = month_data['month']
            from_name = 'SIPPEC'
            sheet_name = "Rapport_Mailing_Mensuel_{0}_{1}_{2}".format(from_name.upper(), month_str, year_str)
            self.current_sheet_name = sheet_name
            # month_data['sent_date'] = month_data['sent_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

            month_df = pd.DataFrame(month_data).transpose()

            # self.write_dataframe(sheet_name=self.current_sheet_name, dataframe=month_df, start_cell='D1')
            data = [month_df.columns.tolist()] + month_df.values.tolist()

            self.write_data(sheet_name=self.current_sheet_name, data=data, start_cell='D1')
            self.write_dataframe_pivot(aggregated_df=month_df)

            print(f"Données agrégées pour le {month_str}-{year_str} écrites dans la feuille : {sheet_name}")

    def save_flushed_sheet(self, dataframe, all_flush=False):
        if all_flush:
            sheet_all_name = 'SIPPEC Campagnes Mailing'
            self.write_dataframe(sheet_name=sheet_all_name, dataframe=dataframe, start_cell="A1")

        aggregated_df = self.aggregated_stats_by_month(dataframe)
        self.write_aggregated_stats_by_month_to_sheets(aggregated_df=aggregated_df)

    def read_data(self, sheet_name, range_name="A1"):
        sheet = self.get_sheet(sheet_name)
        if sheet:
            try:
                values = sheet.get(range_name)
                print("Données lues avec succès.")
                return values
            except Exception as e:
                print(f"Erreur lors de la lecture des données depuis la feuille : {e}")
        return None

    @staticmethod
    def save_dataframe(dataframe):
        folder = 'Resultat/Data'
        if not os.path.exists(folder):
            os.makedirs(folder)

        excel_file = f"Rapport_Mailing_{(dataframe['from_name'][0]).upper()}_{dataframe['month'][0]}_{dataframe['year'][0]}.xlsx"
        csv_file = f"Rapport_Mailing_{(dataframe['from_name'][0]).upper()}_{dataframe['month'][0]}_{dataframe['year'][0]}.csv"

        path_excel_file = os.path.join(folder, excel_file)
        path_csv_file = os.path.join(folder, csv_file)

        try:
            # Enregistrer dans un fichier Excel
            dataframe.to_excel(path_excel_file, index=False)
            print(f"Données enregistrées avec succès dans {path_excel_file}")

            # Enregistrer dans un fichier CSV
            dataframe.to_csv(path_csv_file, index=False)
            print(f"Données enregistrées avec succès dans {path_csv_file}")

            return path_excel_file, path_csv_file
        except Exception as e:
            print(f"Erreur lors de l'enregistrement des données : {e}")


if __name__ == "__main__":
    API_KEY = '107843423507637176712'
    CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    SPREADSHEET_NAME = 'SIPPEC Reporting Mailing Data'
    sheet_service = GoogleSheetsService(CREDENTIALS_PATH, SPREADSHEET_NAME)
