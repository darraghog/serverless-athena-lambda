'use strict';

const AWS = require('aws-sdk');

var athena = new AWS.Athena();

const ATHENA_BUCKET_NAME = process.env.ATHENA_BUCKET_NAME;
const DATABASE_NAME = process.env.DATABASE_NAME;
const BUCKET_NAME = process.env.BUCKET_NAME;
const TABLE_NAME = 'products';

// Column structure of our table
const PRODUCT_SCHEMA = '(productId STRING, name STRING, color STRING)';


module.exports.searchProductByName = async (name) => {
    const searchQuery = "SELECT * FROM "+DATABASE_NAME+"."+TABLE_NAME+" WHERE name='"+ name +"' ";
    console.log("Running query: " + searchQuery);
    var queryExecutionIdSearch;
    try {
        queryExecutionIdSearch = await startQueryExecutionAthena(searchQuery);
    } catch (error) {
        console.log("Exception: "+error);
        throw(error)
    }
    console.log("Now collecting results...for "+queryExecutionIdSearch);
    return getQueryResults(queryExecutionIdSearch);
}

module.exports.init = async (event) => {
    //try {
        let success_db = await createDatabase(DATABASE_NAME, ATHENA_BUCKET_NAME);
        if (!success_db) {
            return {
                statusCode: 400,
                body: 'Database failed to create'
            }
        }
        let success_table = await createTable(DATABASE_NAME, TABLE_NAME, BUCKET_NAME);
        if (!success_table) {
            return {
                statusCode: 400,
                body: 'Table failed to create'
            }
        }
        return {
            statusCode: 200,
            body: 'Database and table created successfully'
        }
    //} catch (error) {
    //   return {
    //      statusCode: 400,
    //      body: error
    //    };
    //}
}

 async function startQueryExecutionAthena(query) {
    console.log("StartQueryExecutionAthena: "+query);
    return new Promise((resolve, reject) => {
        var params = {
            "QueryString": query,
            "ResultConfiguration" : {
                "EncryptionConfiguration": {
                    "EncryptionOption": "SSE_S3" 
                },
                "OutputLocation" : "s3://"+ATHENA_BUCKET_NAME
            }
        }
        /* Make API call to start the query execution */
        athena.startQueryExecution(params, (err, results) => {
            if (err) {
                console.log("Error returned with params = " + JSON.stringify(params));
                return reject(err)
            }
            /* If successful, get the query ID and queue it for polling */
            console.log("Successfully started " + query + ": " + results.QueryExecutionId)
            return resolve(results.QueryExecutionId);
        })
    })
}

 async function getQueryResults(queryExecutionId) {
    var executionDone = false;
    var params = {
        "QueryExecutionId" : queryExecutionId
    }
    while (!executionDone) {
        executionDone = await isExecutionDone(params);
        console.log('waiting...')
        sleep(2000);
    }
    
    const results = await athena.getQueryResults(params).promise();
    return Promise.resolve(formatResults(results));
 }

 // same as getQueryResults, but we dispense with formatting of the results
 // intended for DML queries where we only care about success or failure
 async function getDMLResults(queryExecutionId) {
   
    var params = {
        "QueryExecutionId" : queryExecutionId
    }
    var executionDone = isExecutionDone(params);
    while (!executionDone) {
        console.log('waiting...')
        sleep(2000);
        executionDone = await isExecutionDone(params);
    }
   
    console.log("Getting query results with params " + JSON.stringify(params))
    const results = await athena.getQueryResults(params).promise();
    console.log("getDMLResults="+results)
    return Promise.resolve(results);
 }

 async function isExecutionDone(params) {
    console.log("Running getQueryExecution with queryExecutionId="+params.QueryExecutionId)
    const result = await athena.getQueryExecution(params).promise();
    console.log("Completed running getQueryExecution:"+result.QueryExecution.Status.State)
    if (result.QueryExecution.Status.State === 'SUCCEEDED') {
        return Promise.resolve(true);
    } 
    if (result.QueryExecution.Status.State == 'FAILED') {
        // If failed, then execution is done, but we need to know why it failed for logging purposes
        console.log("Execution failed: " + result.QueryExecution.Status.StateChangeReason);
        return Promise.resolve(true);
    }
    return Promise.resolve(false);
 }

async function createDatabase(dbName,location) {
    const query = "CREATE DATABASE IF NOT EXISTS " + dbName + " LOCATION 's3://"+location+"'";
    console.log(query)
    try {
        const queryExecutionId = await startQueryExecutionAthena(query);
        var result = await getDMLResults(queryExecutionId);
        return Promise.resolve(true)
   
    } catch(error) {
        console.log("Error returned in createDatabase: " + error);
    }
    return Promise.resolve(false);
}

async function createTable(dbName, tableName, location) {
    var query = "CREATE EXTERNAL TABLE IF NOT EXISTS " + dbName+"."+tableName + " " + PRODUCT_SCHEMA;
    query = query + " ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')"
    query = query + " LOCATION 's3://"+location+"/'"; // all subfolders will be automatically included
    console.log("createTable Query="+query)
    try {
        const queryExecutionId = await startQueryExecutionAthena(query);
        const results = await getDMLResults(queryExecutionId)

        return Promise.resolve(true); // errors will be caught by try/catch clause
    } catch (error) {
        console.log("Error creating table: "+ error);
    }
    return Promise.resolve(false);  
}


 function formatResults(results) {
    var formattedResults = [];
   
    const rows = results.ResultSet.Rows;
    rows.forEach(function(row) {         
      var value = {
            productId: row.Data[0].VarCharValue,
            name: row.Data[1].VarCharValue,
            color: row.Data[2].VarCharValue
      };
      formattedResults.push(value);
    });
   
    return formattedResults;
 }

 function sleep(delay) {
    var start = new Date().getTime();
    while (new Date().getTime() < start + delay);
}