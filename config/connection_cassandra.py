from astrapy import DataAPIClient
from config.create_collections import create_collection

def connection_cassandra():
    try:
        # Initialize the client
        client = DataAPIClient("AstraCS:vqQtEwnhDpNRxPICFbUFUJyr:d0342ee0cc095ad9f8fb5b5fedef485fe9b698aba78cfc0ed67b55501bbfc0ea")
        db = client.get_database_by_api_endpoint(
            "https://8e436160-51c6-49eb-9a12-a3b72ef8fd05-us-east-2.apps.astra.datastax.com"
        )
        
        print(f"Connected to Astra DB: {db.list_collection_names()}")
        create_collection(db)
        return db
    except Exception as e:
        print(f'Erro ao conectar ao AstraDB: {e}')
        return None
