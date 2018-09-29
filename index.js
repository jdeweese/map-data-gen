const fs = require('fs');
const aws = require('aws-sdk');
const parse = require('csv-parse');
const transform = require('stream-transform');
const googleMapsClient = require('@google/maps').createClient({
    key: 'AIzaSyAtUtT2rbVq6Se93f-bbU6d9oF3D_fISGA',
    Promise: Promise
  });


var csvFormat = [];

function readAddressesFromFile(file){
    let src_file = fs.createReadStream(file);
 
    let parser = parse(
        {
            columns: true,
            trim: true, 
            skip_lines_with_error: true
        });
    let transformer = transform((data)=>transformCsvAddress(data));

    src_file.pipe(parser).pipe(transformer).pipe((address)=>lookupCompleteAddress(address))//.pipe(process.stdout);

}

function transformCsvAddress (data){
    let stringOutput = null;
    if (data.STREET == "" ) minimizedData = null// skip entries with blank addresses
    else { // only copy lat, long and street address
        let minimizedData = { 
            lat: data.LAT,
            lon: data.LON,
            address: data.NUMBER +' '+ data.STREET
        };
        stringOutput = JSON.stringify(minimizedData);
    }
    return stringOutput
}

function lookupCompleteAddress (partialAddress){
    
    googleMapsClient.placesAutoComplete({
        input: partialAddress.address,
        location: partialAddress.lat +','+ partialAddress.lon
    }).asPromise().then((response)=>{return response});
}

readAddressesFromFile ('./las_vegas_area_addresses.csv');