'use strict';

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const uuidv1 = require('uuid/v1');
const FOLDER_NAME = 'raw';
const BUCKET_NAME = process.env.BUCKET_NAME;

module.exports.saveProduct = async product => {
   product.productId = uuidv1();
   const params = {
       Body: JSON.stringify(product),
       Bucket: BUCKET_NAME,
       Key: `${FOLDER_NAME}/${product.productId}`,
       ContentType: "application/json"
   };  
   return s3.putObject(params).promise();
}

