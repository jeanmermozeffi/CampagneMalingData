import re
import requests
from datetime import datetime, timedelta
import pandas as pd
import calendar

from credential.secrets import keys


class VboutApi:
    def __init__(self):
        self.api_key = keys.get('API_KEY_VBOUT', None)
        self.base_url = 'https://api.vbout.com/1/emailmarketing/campaigns.json'
        self.url = f"https://api.vbout.com/1/emailmarketing/campaigns.json?key={self.api_key}&filter=all&limit={5000}"

        # self.data_response = self.get_campaigns_data()

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
            day=calendar.monthrange(
                last_day_previous_month.year,
                last_day_previous_month.month)[1]
        )

        return first_day_previous_month.strftime("%m/%d/%Y"), last_day_previous_month.strftime("%m/%d/%Y")

    @staticmethod
    def convert_string_to_date(date_string):
        date_object = datetime.strptime(date_string, '%b %d %Y').date()
        date_object = date_object.strftime('%d-%m-%Y')

        return date_object

    @staticmethod
    def get_response(url=None, params=None):
        try:
            response = requests.get(url=url, params=params)
            response.raise_for_status()

            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return None

    @staticmethod
    def extract_date_components(date_string):
        try:
            # Convertir la chaîne en datetime
            date_time = pd.to_datetime(date_string, format='%m/%d/%Y %H:%M')

            # Extraire l'année, le mois et le jour
            year = str(date_time.year)
            month = str(date_time.month)
            day_month = str(date_time.day)

            return year, month, day_month

        except ValueError as e:
            # Gérer l'erreur si la conversion échoue
            print(f"Erreur lors de la conversion de la chaîne en objet datetime : {e}")
            return None, None, None

    def get_campaigns_data(self, filter_param="all", from_date=None, to_date=None, limit=5000, all_campaigns=False):
        if not all_campaigns and from_date is None and to_date is None:
            from_date, to_date = self.previous_month_dates()

        params = {
                'key': self.api_key,
                'filter': filter_param,
                'from': from_date,
                'to': to_date,
                'limit': limit
            }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()

            data = response.json()
            campaign_number = data["response"]["data"]["campaigns"]["count"]

            campaign_data_list = []

            if campaign_number:
                for item in data["response"]["data"]["campaigns"]["items"]:
                    if item.get("sent_date"):
                        year, month, day_month = self.extract_date_components(item["sent_date"])
                        campaign_data = {
                            "id": item.get("id"),
                            "creation_date": item.get("creation_date"),
                            "subject": item.get("subject"),
                            "sent_date": item.get("sent_date"),
                            "year": year,
                            "month": month,
                            "day_month": day_month,
                            "delivery_success": item.get("delivery_success", 0),
                            "delivery_failed": item.get("delivery_failed", 0),
                            "from_name": item.get("from_name"),
                            # "unsubscribed": item.get("unsubscribed", 0),
                        }

                        campaign_data_list.append(campaign_data)

                return campaign_data_list, from_date, to_date
            else:
                print("Aucune campagne trouvée.")
                return None, None, None
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")

    @staticmethod
    def get_title(data_list, campaign_id):
        if "response" in data_list and "data" in data_list["response"] and "item" in data_list["response"]["data"]:
            item_data = data_list["response"]["data"]["item"]

            if item_data and "name" in item_data:
                title = item_data["name"]
                return title
            else:
                print(f"Le champ 'name' est manquant dans les données de la campagne avec l'ID {campaign_id}")
                return None
        else:
            print(f"Impossible de récupérer le titre de la campagne avec l'ID {campaign_id}")
            return None

    def get_campaign_titles(self, campaign_ids):
        campaign_titles = []
        params = {
            'key': self.api_key,
            'type': 'standard'
        }

        for campaign_id in campaign_ids:
            params['id'] = campaign_id

            try:
                data_list = self.get_response(url='https://api.vbout.com/1/emailmarketing/getcampaign.json', params=params)

                if isinstance(data_list, dict) and data_list:
                    title = self.get_title(data_list, campaign_id)
                    if title is not None:
                        campaign_titles.append(title)
            except requests.exceptions.RequestException as e:
                print(f"Erreur lors de la requête API : {e}")

        return campaign_titles

    def extract_clicks_all_mail(self, liste_mails):
        resultats = {}

        for mail in liste_mails:
            pattern = re.search(r'(\d+) clicks', mail)
            match = pattern

            if match:
                nombre_clics = int(match.group(1))
                if '@' in mail:
                    adresse_email = mail.split('(')[0]
                    resultats[adresse_email] = resultats.get(adresse_email, 0) + nombre_clics

        return resultats

    def extract_clicks(self, mail):
        match = re.search(r'(\d+) clicks', mail)
        if match:
            return int(match.group(1))
        else:
            return 0

    @staticmethod
    def total_clicks(dictionary_clicks):
        return sum(dictionary_clicks)

    @staticmethod
    def total_unique_clicks(dictionary_clicks):
        return sum(1 for click in dictionary_clicks.values() if click == 1)

    def process_clicks(self, campaign_stats_click, verbose=False):
        total_all_clicks = 0
        total_unique_clicks_result = 0

        for entry in campaign_stats_click:
            clickers = entry.get('clickers', [])

            clicks = {}
            unique_clicks = {}

            for clicker in clickers:
                click_count = self.extract_clicks(clicker)
                clicks[clicker] = click_count

                if click_count == 1:
                    unique_clicks[clicker] = click_count

            total_entry_clicks = self.total_clicks(clicks.values())
            total_entry_unique_clicks = self.total_unique_clicks(unique_clicks)

            if verbose:
                print(f"Pour URL {entry['url']}:")
                print(f"  Total Clics: {total_entry_clicks} clics")
                print(f"  Total Clics Unique: {total_entry_unique_clicks} clics\n")

            total_all_clicks += total_entry_clicks
            total_unique_clicks_result += total_entry_unique_clicks

        if verbose:
            print("Totaux globaux:")
            print(f"  Nombre total de clics pour toutes les URL: {total_all_clicks} clics")
            print(f"  Total de clics uniques pour toutes les URL: {self.total_unique_clicks} clics")

        return total_all_clicks, total_unique_clicks_result

    @staticmethod
    def extract_numeric_value(input_string):
        match = re.match(r'([\d.]+)\s', input_string)

        # Vérifier si la correspondance est trouvée
        if match:
            return match.group(1)
        else:
            return None

    @staticmethod
    def cleaned_stats(campaign_stats):
        for campaign_stat in campaign_stats.values():
            return self.extract_numeric_value(campaign_stat)

    def get_campaigns_stats(self, campaign_ids):
        stats = []
        errors_campaigns = []
        total_all_clicks_stats = []
        total_unique_clicks_stats = []

        params = {
            'key': self.api_key,
            'type': 'standard'
        }

        for campaign_id in campaign_ids:
            params['id'] = campaign_id

            try:
                data = self.get_response(url='https://api.vbout.com/1/emailmarketing/stats.json', params=params)
                campaign_data = data.get("response", {}).get("data", {}).get("campaign", {})

                if campaign_data:
                    campaign_stats = campaign_data.get("stats", {})
                    if campaign_stats:
                        cleaned_stat = {key: int(self.extract_numeric_value(value)) for key, value in campaign_stats.items()}
                        stats.append(cleaned_stat)

                    campaign_stats_click = campaign_data.get("clicks", {})
                    if campaign_stats_click:
                        total_all_click, total_unique_click = self.process_clicks(campaign_stats_click)
                        total_all_clicks_stats.append(total_all_click)
                        total_unique_clicks_stats.append(total_unique_click)

            except requests.exceptions.RequestException as e:
                print(f"Erreur lors de la requête API pour la campagne avec l'ID {campaign_id}: {e}")
                errors_campaigns.append(campaign_id)

        return stats, total_all_clicks_stats, total_unique_clicks_stats, errors_campaigns

    @staticmethod
    def merge_lists_dict(*lists):
        # Vérifier que toutes les listes ont la même longueur
        lengths = [len(lst) for lst in lists]
        if len(set(lengths)) != 1:
            raise ValueError("Toutes les listes doivent avoir la même longueur.")

        # Fusionner les listes en ajoutant les éléments des listes suivantes à la première liste
        merged_list = []
        for items in zip(*lists):
            merged_item = {}
            for item in items:
                merged_item.update(item)
            merged_list.append(merged_item)

        return merged_list

    @staticmethod
    def add_to_list(existing_list, new_lists, label):
        # Assurez-vous que la longueur de la liste des titres correspond à la longueur de la liste existante
        if len(existing_list) != len(new_lists):
            raise ValueError("Les listes n'ont pas la même longueur.")

        # Utilisez l'index pour accéder simultanément aux éléments des deux listes
        merged_list = [
            {**item, label: title}
            for item, title in zip(existing_list, new_lists)
        ]

        return merged_list

    def create_dataframe(self, data):
        df = pd.DataFrame(data)

        # Convertissez les colonnes appropriées en types de données numériques
        numeric_columns = ['delivery_success', 'delivery_failed', 'unsubscribed', 'open', 'unopened', 'bounced', 'total_clicks', 'total_clicks_unique']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        self.convert_datetime_columns_to_string(df)
        df['total_deliveries'] = df[['delivery_success', 'delivery_failed']].sum(axis=1)

        return df

    @staticmethod
    def convert_datetime_columns_to_string(dataframe):
        for column in dataframe.columns:
            if pd.api.types.is_datetime64_any_dtype(dataframe[column]):
                dataframe[column] = dataframe[column].dt.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def check_list_lengths(*lists):
        """
        Vérifie si toutes les listes dans la liste de listes ont la même longueur.

        Args:
            *lists (list): Liste de listes à vérifier.

        Returns:
            bool: True si toutes les listes ont la même longueur, False sinon.
        """
        lengths = [len(lst) for lst in lists]

        return all(length == lengths[0] for length in lengths)

    @staticmethod
    def check_list_types(*lists):
        """
        Vérifie si tous les éléments dans chaque liste ont le même type.

        Args:
            *lists (list): Liste de listes à vérifier.

        Returns:
            bool: True si tous les éléments ont le même type dans chaque liste, False sinon.
        """
        types = [set(map(type, lst)) for lst in lists]

        return all(t == types[0] for t in types)

    @staticmethod
    def convert_to_int(data, keys):
        for entry in data:
            for key in keys:
                if key in entry:
                    entry[key] = int(entry[key])
        return data


if __name__ == '__main__':
    vbout = VboutApi()
    from_to = '07/01/2024'
    to_end = '07/31/2024'
    campaign_data_list, from_date, to_date = vbout.get_campaigns_data(from_date=from_to, to_date=to_end)
    print("**"*60)
    if campaign_data_list is not None:
        print(len(campaign_data_list))


