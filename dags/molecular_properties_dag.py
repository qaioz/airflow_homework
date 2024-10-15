from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pandas as pd
from rdkit import Chem
from rdkit.Chem import Descriptors, Crippen
from minio import Minio
import os

DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
MINIO_ENDPOINT = 'storage:9000'
MINIO_ACCESS_KEY = 'minio_access_key'
MINIO_SECRET_KEY = 'minio_secret_key'
BUCKET_NAME = 'bronze'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    'smiles_to_s3',
    default_args=default_args,
    description='A DAG that processes SMILES and uploads results to MinIO',
    schedule_interval='@daily',
    catchup=False
)

# Extract task
def extract_data(**kwargs):
    engine = create_engine(DATABASE_URI)
    query = """
    SELECT smiles, related_col FROM molecules
    WHERE date = '{{ ds }}'
    """
    df = pd.read_sql(query, engine)
    return df.to_dict()

# Transform task
def transform_data(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='extract_data')
    df = pd.DataFrame(df_dict)
    
    def calculate_properties(smiles):
        mol = Chem.MolFromSmiles(smiles)
        mol_weight = Descriptors.MolWt(mol)
        logp = Crippen.MolLogP(mol)
        tpsa = Descriptors.TPSA(mol)
        h_donors = Descriptors.NumHDonors(mol)
        h_acceptors = Descriptors.NumHAcceptors(mol)
        lipinski_pass = (
            mol_weight <= 500 and
            logp <= 5 and
            h_donors <= 5 and
            h_acceptors <= 10
        )
        return mol_weight, logp, tpsa, h_donors, h_acceptors, lipinski_pass

    # Apply the transformation
    df[['MolecularWeight', 'LogP', 'TPSA', 'HDonors', 'HAcceptors', 'LipinskiPass']] = df['smiles'].apply(lambda x: calculate_properties(x)).apply(pd.Series)
    
    # Save transformed data in XCom
    ti.xcom_push(key='transformed_data', value=df.to_dict())

# Save and upload task
def save_and_upload(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.DataFrame(df_dict)
    
    # Save DataFrame to an Excel file
    file_path = '/tmp/molecules_data.xlsx'
    df.to_excel(file_path, index=False)

    # Initialize MinIO client and upload file
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    client.fput_object(
        BUCKET_NAME, 'molecules_data.xlsx', file_path
    )
    
    # Cleanup
    os.remove(file_path)

# Define tasks
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

save_and_upload_task = PythonOperator(
    task_id='save_and_upload',
    python_callable=save_and_upload,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_data_task >> transform_data_task >> save_and_upload_task
