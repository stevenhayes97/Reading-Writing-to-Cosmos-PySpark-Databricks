import pydocumentdb
from pydocumentdb import document_client
from pydocumentdb import documents
import json

# You can find these values in the Cosmos DB Connection Strings in the Azure Portal

masterKey = 'insert master key'
host =  'insert host'

connectionPolicy = documents.ConnectionPolicy()
connectionPolicy.EnableEndpointDiscovery 
connectionPolicy.PreferredLocations = ["Central US", "East US 2", "Southeast Asia", "Western Europe","Canada Central"]
client = document_client.DocumentClient(host, {'masterKey': masterKey}, connectionPolicy)

# Just some dummy data
data = spark.createDataFrame([Row(test1='xxxxx', test2='yyyyyy'),
 Row(test1='asdfasdf', test2='dasffaac'),
 Row(test1='jjjfjf', test2='fffee3'),
 Row(test1='xzsef4', test2='farq2345w')])
 
 writeConfig = {
 "Endpoint" : host,
 "Masterkey" : masterKey,
 "Database" : "Insert your Database Name here",
 "Collection" : "Insert your Collection ID here",
 "Upsert" : "true"
}

 # Make sure you have the JAR file installed, or this command won't work
data.write.format("com.microsoft.azure.cosmosdb.spark").mode("append").options(**writeConfig).save()
