import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents

# These values can be found in the Azure Portal under the Cosmos DB Connection Strings
masterKey = 'Insert your masterKey here'
host =  'Insert your host here'

connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
connectionPolicy.PreferredLocations = ["Central US", "East US 2", "Southeast Asia", "Western Europe","Canada Central"]
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)

databaseId = 'Insert Database ID'
collectionId = 'Insert Collection ID'

dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId

querystr = "SELECT * from c"

query = client.QueryDocuments(collLink, querystr, options= { 'enableCrossPartitionQuery': True }, partition_key=None)

cosmoselements = list(query)

cosmos_data_df = spark.createDataFrame(cosmoselements)
