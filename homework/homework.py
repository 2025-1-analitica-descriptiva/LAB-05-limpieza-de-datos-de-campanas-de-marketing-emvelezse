"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import glob
import os 
def read_files(input_directory):
    dataframes = []

    # Busca todos los archivos ZIP en el directorio
    files = glob.glob(f"{input_directory}*")
    print(files)
    for file in files:
        # Lee el archivo CSV dentro del ZIP
        df = pd.read_csv(file)
        dataframes.append(df)

    # Concatena todos los DataFrames en uno solo
    final_df = pd.concat(dataframes, ignore_index=True)
    return final_df

def clean_client(df_client: pd.DataFrame):

    df_client["job"] = df_client["job"].str.replace("-", "_").str.replace(".", "")
    df_client["education"] = df_client["education"].str.replace(".", "_").replace("unknown", pd.NA)
    df_client["credit_default"] = df_client["credit_default"].agg(lambda x: 1 if x=="yes" else 0)
    df_client["mortgage"] = df_client["mortgage"].agg(lambda x: 1 if x=="yes" else 0)

    return df_client

def clean_compaign(df_campaign : pd.DataFrame):

    df_campaign["previous_outcome"] = df_campaign["previous_outcome"].agg(lambda x: 1 if x=="success" else 0)
    df_campaign["campaign_outcome"] = df_campaign["campaign_outcome"].agg(lambda x: 1 if x=="yes" else 0)
    df_campaign['month_num'] = pd.to_datetime(df_campaign['month'], format='%b').dt.month
    df_campaign['last_contact_date'] = pd.to_datetime({
    'year': 2022,
    'month': df_campaign['month_num'],
    'day': df_campaign['day']
    })  
    df_campaign = df_campaign.drop(['month_num', 'month', 'day'], axis=1)

    return df_campaign

def save_output(name:str, dataframe:pd.DataFrame):
    output_directory = "files/output"
    if not os.path.exists(output_directory):    
        os.makedirs(output_directory)
    dataframe.to_csv(f"{output_directory}/{name}", index=False)
    

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    df = read_files("files/input/")
    df_client = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    df_client = clean_client(df_client)

    df_campaign = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "month", "day"]]
    df_campaign = clean_compaign(df_campaign)

    df_economics = df[["client_id", "cons_price_idx", "euribor_three_months"]]

    save_output("client.csv", df_client)
    save_output("campaign.csv", df_campaign)
    save_output("economics.csv", df_economics)


if __name__ == "__main__":
    clean_campaign_data()
