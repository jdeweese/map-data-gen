const fs = require('fs');
const aws = require('aws-sdk');
const parse = require('csv-parse');
const transform = require('stream-transform');
const randomdate = require('random-datetime');
const datetime = require('date-and-time');
const stringify = require('csv-stringify');
const rfs    = require('rotating-file-stream');
const rimraf = require('rimraf');
const random = require('random-number');

const temp_dir = './outputs';

// clean out temp outputs directory
rimraf.sync(temp_dir,{},()=>{console.log('deleted old temp outputs')} );

function randomIntInc(low, high) {
  return Math.floor(Math.random() * (high - low + 1) + low)
}

// object of configurable constants to set defaults in the output
const conf = {
    city_name: "Las Vegas",
    state_code: "NV",
    truckid: 2,
    warehouse_street: "12 Business Park Court",
    warehouse_city: "Las Vegas",
    warehouse_state: "NV",
    warehouse_zip: "89128",
    warehouse_lat: "36.213389",
    warehouse_lon: "-115.25028",
    product_name: "1 dozen roses",
    product_delivered: "1 dozen roses",
    // order_year: "2018"
}

var prices = ['9.99', '12.49', '19.99', '24.99', '5.65', '32.49', '59.99', '45.00', '52.25', '28.99'];

var zip_codes = ["89179", "89109", "89138", "89143", "89166", "89144", "89146", "89118", "89145", "89120", "89134", "89141", "89178", "89139", "89130", "89128", "89183", "89148", "89131", "89101", "89147", "89129", "89123", "89121", "89110"];

var id = 1; // global scope variable to track customer id
var orderid = 1000; 

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
    let minimizedData = null;

    if (data.STREET != "" ) // only create entries for lines including address 
	{

	var rn = require('random-number');
	var gen = rn.generator({
  		min: 0
		, max: 9
		, integer: true
	});
        var orderDT = randomdate({
                        year: 2015,
                        month: 2
                });
        var promisedDT = new Date(orderDT);
        let promDT = datetime.addDays(promisedDT, 1);
        var randomZipInt = randomIntInc(0,21);
 	var randomPriceInt = randomIntInc(0,9);
        minimizedData = { // only copy lat, long and street address

 	    orderdate: datetime.format(orderDT,
                'YYYY-MM-DDTHH:mm'),
	    address: data.number +' '+ data.street.replace(/ +(?= )/g,''),
	    city: conf.city_name,
            state: conf.state_code,
            zip: zip_codes[randomZipInt],
            lat: data.lat,
            lon: data.lon,
            // regex to take out extra spaces inside string
	    orderid: orderid++,
            customer_id: id++,
            price: prices[randomPriceInt],
            truck: conf.truckid,
            warehouse_address: conf.warehouse_street,
            warehouse_city: conf.warehouse_city,
            warehouse_state: conf.warehouse_state,
            warehouse_zip: conf.warehouse_zip,
            warehouse_lat: conf.warehouse_lat,
            warehouse_lon: conf.warehouse_lon,
            product: conf.product_name,
	    productdelivered: conf.product_delivered,
            promiseddate: datetime.format(promDT,
                'YYYY-MM-DDTHH:mm'),
	    actualdate: datetime.format(promDT,
		'YYYY-MM-DDTHH:mm')
	    }

        }
    return minimizedData;
}

// runs a test for local file
genSampleAddresses('../../Documents/Projects/addresses_q1_2015.csv', 'addresses_q1_2015_out.csv');
