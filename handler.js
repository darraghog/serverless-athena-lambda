'use strict';
const productManager = require('./productManager');
const myathena = require('./athena')

module.exports.createProduct = async (event) => {
 const product = JSON.parse(event.body);
 try {
   await productManager.saveProduct(product);
   
   return {
     statusCode: 200,
     body: 'Product was saved in the storage'
   };
 } catch (error) {
   return {
     statusCode: 400,
     body: error
   };
 }
};

module.exports.searchProductByName = async (event) => {
  var name;
  if (event.queryStringParameters) {
    name =  event.queryStringParameters.name;
  } else {
    try {
      // Assume JSON object
      name = event.name;
    } catch (error) {
        return {
        statusCode: 400,
        body: error
        };
      }
    }
  try {
    const result = await myathena.searchProductByName(name);

    return {
      statusCode: 200,
      body: JSON.stringify(result)
    };
  } catch (error) {
      return {
        statusCode: 400,
        body: error
      };
    }
  }