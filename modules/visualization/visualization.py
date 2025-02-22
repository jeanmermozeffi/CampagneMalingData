import plotly.graph_objects as go
import pandas as pd
import numpy as np
from modules import data_test


class DataVisualizer:
    def __init__(self, current_data, data_previous_year: dict, data_current_year: dict):
        self.data = current_data
        self.data_previous_year = data_previous_year
        self.data_current_year = data_current_year

    def mean(self):
        """Calcule la moyenne des données."""
        return np.mean(self.data)

    def median(self):
        """Calcule la médiane des données."""
        return np.median(self.data)

    def mode(self):
        """Calcule la ou les valeurs modales des données."""
        # return pd.Series(self.data).mode().tolist()
        return pd.Series(self.data).mode().tolist()

    def std_dev(self):
        """Calcule l'écart-type des données."""
        return np.std(self.data, ddof=1)

    def variance(self):
        """Calcule la variance des données."""
        return np.var(self.data, ddof=1)

    def min_value(self):
        """Trouve la valeur minimale dans les données."""
        return np.min(self.data)

    def max_value(self):
        """Trouve la valeur maximale dans les données."""
        return np.max(self.data)

    def range_value(self):
        """Calcule l'étendue (range) des données."""
        return np.max(self.data) - np.min(self.data)

    def percentiles(self, percentiles_list):
        """Calcule les percentiles spécifiés des données."""
        return np.percentile(self.data, percentiles_list)

    @staticmethod
    def summary_statistics(self):
        """Retourne un résumé statistique complet des données."""
        return {
            "mean": self.mean(),
            "median": self.median(),
            "mode": self.mode(),
            "std_dev": self.std_dev(),
            "variance": self.variance(),
            "min": self.min_value(),
            "max": self.max_value(),
            "range": self.range_value(),
            "percentiles": {
                "25th": self.percentiles(25),
                "50th": self.percentiles(50),
                "75th": self.percentiles(75)
            }
        }

    def plot_delivery_stats(self):
        fig = go.Figure()
        fig.add_trace(go.Bar(x=['Success', 'Failed'], y=[self.data['delivery_success'], self.data['delivery_failed']],
                             name="Statistiques d'envoi"))
        fig.update_layout(title='Delivery Success vs Failed',
                          xaxis_title='Type',
                          yaxis_title='Count')
        return fig

    def plot_open_vs_unopened(self):
        fig = go.Figure()
        fig.add_trace(go.Pie(labels=['Opened', 'Unopened'],
                             values=[self.data['open'], self.data['unopened']],
                             hole=0.3))
        fig.update_layout(title='E-mails Ouverts vs Non Ouverts')
        return fig

    def plot_bounced_vs_unsubscribed(self):
        fig = go.Figure()
        fig.add_trace(go.Pie(labels=['Bounced', 'Unsubscribed'],
                             values=[self.data['bounced'], self.data['unsubscribed']],
                             hole=0.3))
        fig.update_layout(title='E-mails rejetés vs Désabonnés')
        return fig

    def plot_clicks(self):
        fig = go.Figure()
        fig.add_trace(go.Bar(x=['Total Clicks', 'Unique Clicks'],
                             y=[self.data['total_clicks'], self.data['total_clicks_unique']],
                             name='Clicks Statistics'))
        fig.update_layout(title='Nombre total de clics vs Clics Uniques',
                          xaxis_title='Type',
                          yaxis_title='Count')
        return fig

    def plot_comparison(self, previous_data, current_data):
        metrics = ['delivery_success', 'delivery_failed', 'open', 'unopened', 'bounced', 'unsubscribed', 'total_clicks',
                   'total_clicks_unique']
        previous_values = [previous_data[metric] for metric in metrics]
        current_values = [current_data[metric] for metric in metrics]

        fig = go.Figure()
        fig.add_trace(go.Bar(x=metrics, y=previous_values, name='Previous Date'))
        fig.add_trace(go.Bar(x=metrics, y=current_values, name='Current Date'))

        fig.update_layout(title='Comparaison des mesures entre Juin vs Juillet',
                          xaxis_title='Metrics',
                          yaxis_title='Count',
                          barmode='group')
        return fig

    def plot_overall_status(self):
        labels = ['Unopened', 'Opened', 'Unsubscribed', 'Delivery Failed']
        values = [self.data['unopened'], self.data['open'], self.data['unsubscribed'], self.data['delivery_failed']]

        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.3)])
        fig.update_layout(title='Statut général des e-mails')
        return fig

    # 2. Taux de Changement (%)
    @staticmethod
    def calculate_percentage_change(previous_data, current_data):
        metrics = ['delivery_success', 'delivery_failed', 'open', 'unopened', 'bounced', 'unsubscribed', 'total_clicks',
                   'total_clicks_unique']
        percentage_changes = {}

        for metric in metrics:
            previous = int(previous_data[metric])
            current = int(current_data[metric])
            if previous != 0:
                percentage_change = ((current - previous) / previous) * 100
            else:
                percentage_change = 0
            percentage_changes[metric] = percentage_change

        return percentage_changes

    @staticmethod
    def plot_percentage_change(percentage_changes):
        metrics = list(percentage_changes.keys())
        changes = list(percentage_changes.values())

        fig = go.Figure()
        fig.add_trace(go.Bar(x=metrics, y=changes, name='Percentage Change'))

        fig.update_layout(title='Pourcentage de changement des mesures entre Juin vs Juillet',
                          xaxis_title='Metrics',
                          yaxis_title='Percentage Change (%)')
        return fig

    # 3. Taux d’Ouverture et Taux de Clics
    @staticmethod
    def calculate_rates(data):
        open_rate = (int(data['open']) / (int(data['open']) + int(data['unopened']))) * 100
        click_rate = (int(data['total_clicks_unique']) / (int(data['open']) + int(data['unopened']))) * 100
        return open_rate, click_rate

    def plot_rates_comparison(self, previous_data, current_data):
        previous_open_rate, previous_click_rate = self.calculate_rates(previous_data)
        current_open_rate, current_click_rate = self.calculate_rates(current_data)

        metrics = ['Open Rate', 'Click Rate']
        previous_rates = [previous_open_rate, previous_click_rate]
        current_rates = [current_open_rate, current_click_rate]

        fig = go.Figure()
        fig.add_trace(go.Bar(x=metrics, y=previous_rates, name='Previous Date'))
        fig.add_trace(go.Bar(x=metrics, y=current_rates, name='Current Date'))

        fig.update_layout(title="Comparaison du taux d'ouverture et du taux de clic",
                          xaxis_title='Metrics',
                          yaxis_title='Rate (%)',
                          barmode='group')
        return fig

    def plot_line_chart(self, metric):
        previous_values = self.data_previous_year[metric]
        current_values = self.data_current_year[metric]
        months = self.data_previous_year['months']

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=months, y=previous_values, mode='lines+markers', name='Previous'))
        fig.add_trace(go.Scatter(x=months, y=current_values, mode='lines+markers', name='Current'))

        fig.update_layout(title=f'{metric} Over Months',
                          xaxis_title='Months',
                          yaxis_title='Count')
        return fig

    def plot_all_metrics_over_time(self):
        metrics = ['delivery_success', 'delivery_failed', 'open', 'unopened', 'bounced', 'unsubscribed', 'total_clicks',
                   'total_clicks_unique']
        for metric in metrics:
            yield self.plot_line_chart(metric)

    def plot_bar_chart(self, metric):
        previous_values = self.data_previous_year[metric]
        current_values = self.data_current_year[metric]
        months = self.data_previous_year['months']

        fig = go.Figure()
        fig.add_trace(go.Bar(x=months, y=previous_values, name='Previous Year', marker_color='indianred'))
        fig.add_trace(go.Bar(x=months, y=current_values, name='Current Year', marker_color='lightsalmon'))

        fig.update_layout(title=f'{metric} Comparaison sur plusieurs mois',
                          xaxis_title='Mois',
                          yaxis_title='Valeur',
                          barmode='group')
        return fig

    def plot_all_bar_charts(self):
        metrics = ['delivery_success', 'delivery_failed', 'open', 'unopened', 'bounced', 'unsubscribed', 'total_clicks',
                   'total_clicks_unique']
        for metric in metrics:
            yield self.plot_bar_chart(metric)

    # Calcul des metriques
    def calculate_percentage_change_metric(self, metric):
        previous_values = self.data[metric]
        current_values = self.data[metric]

        percentage_changes = []
        for prev, curr in zip(previous_values, current_values):
            if prev != 0:
                change = ((curr - prev) / prev) * 100
            else:
                change = 0  # or handle the division by zero appropriately
            percentage_changes.append(change)

        return percentage_changes
