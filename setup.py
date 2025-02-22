#!/usr/bin/env python3

import sys
import os
import streamlit as st
import datetime
import time
import calendar
import pandas as pd

from modules.vbout.vboutapi import VboutApi
from modules.sippec.sippecapi import SippecAPI
from modules import data_test
from modules.visualization.visualization import DataVisualizer

from modules.spreadsheets.spreadsheets import GoogleSheetsService
from credential.secrets import sippec, path
from typing import List, Optional, Dict, Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

st.set_page_config(
    page_title="Rapporting Data Storage",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

path_data = 'Resultats/Data/SIPPEC Campagnes Mailing.csv'

df = pd.read_csv(path_data)
st.sidebar.title("Statistique Data Mailing")
pages = ["Contexte du projet", "Exploration des données", "Analyse de données", "Modélisation"]

page = st.sidebar.radio("Aller vers la page :", pages)

# Initialisation de la variable dans l'état de session si elle n'existe pas encore
if 'global_variable' not in st.session_state:
    st.session_state.global_variable = {}


@st.cache_resource()
def connect_spreadsheet(spreadsheet_name: Optional[str] = None):
    if spreadsheet_name is None:
        spreadsheet_name = sippec.get("SPREADSHEET_NAME")

    spreadsheet_service: GoogleSheetsService = GoogleSheetsService(
        os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        spreadsheet_name=spreadsheet_name)
    return spreadsheet_service


def create_vbout_api_instance():
    return VboutApi()


def create_sippec_api_instance():
    return SippecAPI()


@st.cache_resource()
def get_vbout_api_instance():
    return create_vbout_api_instance()


@st.cache_resource()
def get_sippec_api_instance():
    return create_sippec_api_instance()


vbout_api = get_vbout_api_instance()
sip_api = get_sippec_api_instance()
sheet_service = connect_spreadsheet()


@st.cache_data
def get_statistics_data():
    return sip_api.get_sippec_data()


@st.cache_data
def get_statistics_academia_data():
    return sip_api.get_academician_data()


def create_data_df(stats_data):
    vbout = get_vbout_api_instance()  # Récupérer l'instance de VboutApi
    return vbout.create_dataframe(stats_data)


@st.cache_data
def get_data_df(stats_data):
    return create_data_df(stats_data)


@st.cache_data
def show_data(data):
    time.sleep(2)  # This makes the function take 2s to run
    return data


@st.cache_data
def get_data_previous_date():
    first_day_previous_month, last_day_previous_month = vbout_api.previous_month_dates()
    date_previous_obj = datetime.datetime.strptime(first_day_previous_month, "%m/%d/%Y").date()
    current_month_sheet_name = f'Rapport_Mailing_Mensuel_SIPPEC_{date_previous_obj.month}_{date_previous_obj.year}'
    previous_month_sheet_name = (f'Rapport_Mailing_Mensuel_SIPPEC_{int(date_previous_obj.month) - 1}_'
                                 f'{date_previous_obj.year}')
    previous_data_date = sheet_service.get_data(sheet_name=previous_month_sheet_name, start_cell='D', end_cell='O')

    return previous_data_date[0]


@st.cache_data
def get_data_year_between_dates():
    year = datetime.datetime.now().year
    previous_year = datetime.datetime.now().year - 1
    data_previous = sheet_service.get_data_previous_year(previous_year)
    data_current = sheet_service.get_data_previous_year(year)

    return data_previous, data_current


if page == pages[0]:
    # st.write("### Contexte du projet")

    original_title = '<h1 style="font-family: serif; color:white; font-size: 20px;">✨ </h1>'
    st.markdown(original_title, unsafe_allow_html=True)

    # st.image("Image/pimconaut2024.jpg")
    # Set the background image
    url_background_image = 'https://gao.beez-africa.com/bundles/pimcoreadmin/img/login/pimconaut2024.jpg'
    background_image = """
    <style>
    [data-testid="stAppViewContainer"] > .main {
        background-image: url("https://gao.beez-africa.com/bundles/pimcoreadmin/img/login/pimconaut2024.jpg");
        background-size: 100vw 100vh;  # This sets the size to cover 100% of the viewport width and height
        background-position: center;  
        background-repeat: no-repeat;
    }
    </style>
    """

    st.markdown(background_image, unsafe_allow_html=True)

    input_style = """
    <style>
    input[type="text"] {
        background-color: transparent;
        color: #a19eae;  // This changes the text color inside the input box
    }
    div[data-baseweb="base-input"] {
        background-color: transparent !important;
    }
    [data-testid="stAppViewContainer"] {
        background-color: transparent !important;
    }
    </style>
    """
    st.markdown(input_style, unsafe_allow_html=True)

elif page == pages[1]:

    st.write("### Exploration des données")

    st.dataframe(df.head(50))

    st.write("Dimensions du dataframe :")

    st.write(df.shape)

    if st.checkbox("Afficher les valeurs manquantes"):
        st.dataframe(df.isna().sum())

    if st.checkbox("Afficher les doublons"):
        st.write(df.duplicated().sum())

elif page == pages[2]:
    is_data = False
    response_df = None

    start_date_key = "start_date_input"
    end_date_key = "end_date_input"
    campaign_data_list = None

    # Définir la date de début comme le 1er janvier 2010
    jan_1_2010 = datetime.date(2010, 1, 1)

    # Obtenir la date actuelle
    today = datetime.datetime.now()

    next_year = today.year + 1

    # Obtenir le premier jour et le dernier jour du mois précédent
    last_month = today.replace(day=1) - datetime.timedelta(days=1)
    first_day_of_last_month = last_month.replace(day=1)
    last_day_of_last_month = last_month.replace(day=calendar.monthrange(last_month.year, last_month.month)[1])

    # Utiliser la date de début modifiée dans la fonction `date_input`
    selected_dates = st.date_input(
        "Sélectionnez la date du reporting",
        (first_day_of_last_month, last_day_of_last_month),
        min_value=jan_1_2010,
        max_value=last_day_of_last_month,
        format="MM.DD.YYYY",
        key=start_date_key
    )

    if len(selected_dates) == 2:
        start_date = selected_dates[0] if selected_dates else None
        end_date = selected_dates[1] if selected_dates else None

        while start_date is None or end_date is None:
            selected_dates = st.date_input(
                "Sélectionnez la plage de dates du reporting",
                (first_day_of_last_month, last_day_of_last_month),
                min_value=jan_1_2010,
                max_value=last_day_of_last_month,
                format="MM.DD.YYYY",
                key=end_date_key if start_date is not None else start_date_key
            )

            if selected_dates:
                if start_date is None:
                    start_date = selected_dates[0]
                else:
                    end_date = selected_dates[1]

        if len(selected_dates) == 2:
            vbout = get_vbout_api_instance()
            start_date, end_date = selected_dates
            from_date = start_date.strftime("%m/%d/%Y")
            to_date = end_date.strftime("%m/%d/%Y")

            st.write("Date de début:", start_date.strftime("%d/%m/%Y"))
            st.write("Date de fin:", end_date.strftime("%d/%m/%Y"))

            campaign_data_list, from_date, to_date = vbout.get_campaigns_data(from_date=from_date, to_date=to_date)

            if campaign_data_list is not None:
                st.write(f"### Compagne ID: {campaign_data_list[0]['id']} de {campaign_data_list[0]['from_name']}")
                st.write(campaign_data_list)

                campaign_ids = list(map(lambda entry: entry['id'], campaign_data_list))
                campaign_titles = vbout.get_campaign_titles(campaign_ids)
                campaigns_stats, total_all_clicks_stats, total_unique_clicks_stats, errors_campaigns \
                    = vbout.get_campaigns_stats(campaign_ids)
                lists = [campaign_data_list, campaign_titles, campaigns_stats, total_all_clicks_stats,
                         total_unique_clicks_stats]

                if vbout.check_list_lengths(*lists):
                    campaign_stats_data = vbout.merge_lists_dict(campaign_data_list, campaigns_stats)
                    campaign_stats_data = vbout.add_to_list(campaign_stats_data, campaign_titles, label='title')
                    campaign_stats_data = vbout.add_to_list(campaign_stats_data, total_all_clicks_stats,
                                                            label='total_clicks')
                    campaign_stats_data = vbout.add_to_list(campaign_stats_data, total_unique_clicks_stats,
                                                            label='total_clicks_unique')
                    keys_to_convert = [
                        "year", "month", "day_month", "delivery_success", "delivery_failed",
                        "open", "unopened", "bounced", "unsubscribed", "total_clicks", "total_clicks_unique"
                    ]
                    campaign_stats_data = vbout_api.convert_to_int(campaign_stats_data, keys_to_convert)

                    st.session_state.global_variable = {
                        'current_data': {
                            'delivery_success': campaign_stats_data[0]['delivery_success'],
                            'delivery_failed': campaign_stats_data[0]['delivery_failed'],
                            'open': campaign_stats_data[0]['open'],
                            'unopened': campaign_stats_data[0]['unopened'],
                            'bounced': campaign_stats_data[0]['bounced'],
                            'unsubscribed': campaign_stats_data[0]['unsubscribed'],
                            'total_clicks': campaign_stats_data[0]['total_clicks'],
                            'total_clicks_unique': campaign_stats_data[0]['total_clicks_unique'],
                        },
                    }

                    response_df = get_data_df(campaign_stats_data)

                    # response_df = vbout.create_dataframe(campaign_stats_data)
                    st.write(f"### Informations sur les données de la compagne du {from_date} au {to_date}")

                    if len(campaign_data_list) > 1:
                        st.dataframe(show_data(response_df))
                        st.button("Re-run")
                    else:
                        st.table(show_data(campaign_stats_data)[0].items())
                        st.button("Re-run")

                    is_data = True

            else:
                st.write("#### Aucune campagne trouvée pour cette période.")

    if is_data:
        option = st.selectbox(
            "Dans quel fichier sheet souhaiter vous l'enregistrement ?",
            ("Test Data Sippec Streamlit", "SIPPEC Reporting Mailing Data"),
            index=None,
            placeholder="Selectionnez votre fichier...",
        )

        if option == 'Test Data Sippec Streamlit':
            name = option
        else:
            name = sippec.get('SPREADSHEET_NAME')

        spreadsheets_button = st.button('Mise à jour des données en ligne')

        if spreadsheets_button:
            try:
                sheet_service.save_flushed_sheet(dataframe=response_df)
                st.write("Success d'enregistrement !")
            except Exception as e:
                st.write(f"Une erreur s'est produite : {e}")

elif page == pages[3]:
    st.title('Email Statistics Visualization')
    previous_data = get_data_previous_date()
    data_previous_year, data_current_year = get_data_year_between_dates()
    current_data = st.session_state.global_variable.get('current_data')
    if current_data is None:
        st.write(f"Veuillez cliquer sur la section {pages[3]}")

    if current_data:
        visualizer = DataVisualizer(current_data, data_previous_year, data_current_year)

        col1, col2, col3, col4 = st.columns(4)

        metrics = ['delivery_success', 'delivery_failed', 'open', 'unopened', 'bounced', 'unsubscribed', 'total_clicks',
                   'total_clicks_unique']

        for index, metric in enumerate(metrics):
            percentage_changes = visualizer.calculate_percentage_change(previous_data, current_data)
            current_value = current_data[metric]
            percentage_change = percentage_changes[metric]

            if metric == 'delivery_success':
                col1.metric("Envoi de l'e-mail réussi", f"{current_value}", f"{percentage_change:.2f}%")
            elif metric == 'delivery_failed':
                col2.metric("Echec de l'envoi", f"{current_value}", f"{percentage_change:.2f}%")
            elif metric == 'open':
                col3.metric("Ouvrir", f"{current_value}", f"{percentage_change:.2f}%")
            elif metric == 'unopened':
                col4.metric("Non ouvert", f"{current_value}", f"{percentage_change:.2f}%")
            elif metric == 'bounced':
                col1.metric("Rebondi", f"{current_value}", f"{percentage_change:.2f}%")
            elif metric == 'unsubscribed':
                col2.metric("Désabonné", f"{current_value}", f"{percentage_change:.2f}%")
            elif metric == 'total_clicks':
                col3.metric("Nombre total de clics", f"{current_value}", f"{percentage_change:.2f}%")
            elif metric == 'total_clicks_unique':
                col4.metric("Total Clicks Unique", f"{current_value}", f"{percentage_change:.2f}%")

        # Sélecteurs d'année et de mois
        st.title("Analyse Statistique Mensuelle")
        col_statistics_1, col_statistics_2 = st.columns(2)
        current_year = datetime.datetime.now().year
        with col_statistics_1:
            annee = st.selectbox("Choisissez l'année", [current_year] + list(range(current_year - 10, current_year)))
        with col_statistics_2:
            mois_count = st.slider("Nombre de mois", min_value=1, max_value=12, value=12)

        # Calculer les statistiques
        # summary = visualizer.summary_statistics()
        #
        # # Affichage avec Streamlit
        # st.title("Résumé Statistique Descriptif")
        #
        # st.subheader("Statistiques sur 'Delivery Success'")
        # st.write(f"Moyenne: {summary['mean']}")
        # st.write(f"Médiane: {summary['median']}")
        # st.write(f"Mode: {summary['mode']}")
        # st.write(f"Écart-type: {summary['std_dev']}")
        # st.write(f"Variance: {summary['variance']}")
        # st.write(f"Valeur minimale: {summary['min']}")
        # st.write(f"Valeur maximale: {summary['max']}")
        # st.write(f"Étendue: {summary['range']}")
        #
        # st.subheader("Percentiles")
        # st.write(f"25e percentile: {summary['percentiles']['25th']}")
        # st.write(f"50e percentile (Médiane): {summary['percentiles']['50th']}")
        # st.write(f"75e percentile: {summary['percentiles']['75th']}")

        col5, col6, col7 = st.columns(3)

        with col5:
            st.plotly_chart(visualizer.plot_delivery_stats())

        with col6:
            st.plotly_chart(visualizer.plot_open_vs_unopened())

        with col7:
            st.plotly_chart(visualizer.plot_bounced_vs_unsubscribed())

        col8, col9 = st.columns(2)

        with col8:
            st.plotly_chart(visualizer.plot_clicks())

        with col9:
            st.plotly_chart(visualizer.plot_overall_status())

        st.title("1. Comparaison des Performances entre Juin et Juillet")
        col10, col11, = st.columns(2)
        with col10:
            st.plotly_chart(visualizer.plot_comparison(previous_data, current_data))

        with col11:
            percentage_changes = visualizer.calculate_percentage_change(previous_data, current_data)
            st.plotly_chart(visualizer.plot_percentage_change(percentage_changes))
        st.plotly_chart(visualizer.plot_rates_comparison(previous_data, current_data))

        col12, col13, = st.columns(2)
        count = 0
        for plot in visualizer.plot_all_metrics_over_time():
            count += 1
            if count % 2 == 0:
                with col12:
                    st.plotly_chart(plot)
            else:
                with col13:
                    st.plotly_chart(plot)

        for plot in visualizer.plot_all_bar_charts():
            count += 1
            if count % 2 == 0:
                with col12:
                    st.plotly_chart(plot)
            else:
                with col13:
                    st.plotly_chart(plot)
