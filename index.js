const fs = require('fs');
const aws = require('aws-sdk');
const parse = require('csv-parse');
const transform = require('stream-transform');
//const randomDate = require('random-datetime');
const stringify = require('csv-stringify');
const rfs    = require('rotating-file-stream');
const rimraf = require('rimraf');

const temp_dir = './outputs';
// clean out temp outputs directory
rimraf.sync(temp_dir, () => console.log('deleted old temp outputs') );

// object of configurable constants to set defaults in the output
const conf = {
    city_name: "Las Vegas",
    state_code: "NV",
    zipcode: "89102",
    // order_year: "2018"
}

// clean up the temp outputs directory


// reads from source address file from openaddress.com and ouputs a csv file
// of addresses in the format expected for the lab
function genSampleAddresses(inputfile, outputfile){
    let src_file = fs.createReadStream(inputfile); // input stream
 
    let parser = parse( // creates the csv parser with options set
        {
            columns: true,
            trim: true, 
            skip_lines_with_error: true,
        });

    let transformer = transform((data)=>transformAddressCsv(data)); // calls function to transform the data

    var stringifier = stringify( // creates string output with options
        { 
            header: true,
        });

    //let writeStream = fs.createWriteStream(outputfile); // output stream
    let writeStream = rfs(outputfile, {
        path: 'outputs',
        size:'2M',
        rotate: 1,
        compress: true,
    });

    // creates the pipeline and executes
    src_file.pipe(parser).pipe(transformer).pipe(stringifier).pipe(writeStream);
}

// creates the expected output format and substitutes in defaults from the defined constant
function transformAddressCsv(data){
    let stringOutput = null;
    if (data.STREET == "" ) minimizedData = null// skip entries with blank addresses
    else { // only copy lat, long and street address
        let minimizedData = { 
            lat: data.LAT,
            lon: data.LON,
            address: data.NUMBER +' '+ data.STREET,
            city: conf.city_name,
            state: conf.state_code,
            zip: conf.zipcode,
        };
        stringOutput = minimizedData;
        //stringOutput = JSON.stringify(minimizedData);
    }
    return stringOutput
}

// runs a test for local file
genSampleAddresses('las_vegas_area_addresses.csv','addresses.csv');