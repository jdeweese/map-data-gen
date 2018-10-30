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
const geomeasure = require('geo-distance');

const temp_dir = './outputs';

// clean out temp outputs directory
rimraf.sync(temp_dir,{},()=>{console.log('deleted old temp outputs')} );

function randomIntInc(low, high) {
  return Math.floor(Math.random() * (high - low + 1) + low)
}

function notReallyRandom(notRandomNumbers) {
  var idx = Math.floor(Math.random() * notRandomNumbers.length);
  return notRandomNumbers[idx];
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
}

var prices = ['9.99', '12.49', '19.99', '24.99', '5.65', '32.49', '59.99', '45.00', '52.25', '28.99'];

var zip_codes = ["89179", "89109", "89138", "89143", "89166", "89144", "89146", "89118", "89145", "89120", "89134", "89141", "89178", "89139", "89130", "89128", "89183", "89148", "89131", "89101", "89147", "89129", "89123", "89121", "89110"];

var warehouse_locations_street = ["12 Business Park Court", "3001 N. Nellis Blvd", "7110 Placid St."];

var warehouse_locations_city = ["Las Vegas", "Las Vegas", "Las Vegas"];

var warehouse_locations_state = ["NV", "NV", "NV"];

var warehouse_locations_zip = ["89128", "89115", "89119"];

var warehouse_locations_lon = ["36.213389", "36.215090", "36.060274"];

var warehouse_locations_lat = ["-115.25028", "-115.062929", "-115.158569"];

var truck_id = [1, 2, 3];

var id = 1; // global scope variable to track customer id
var ordID = 1000; 
var custID = 1;

function generateRandomizationArray(percentbad) {
// this generates an array from 1-100 of indexes, either 1 or 2.  1 means 
// delivery is not good and we'll randomly index into array of late delivery
// times and find a warehouse not close.  2 means delivery was fine and we'll
// randomly index into array of on-time & early delivery time. 
   var randIndices = new Array();
   for (i = 0; i < percentbad; i++) {
      randIndices[i] = 1;
   }

   for (i = percentbad; i < 100; i++) {
      randIndices[i] = 2;
   }
   return randIndices;
}

// reads from source address file from openaddress.com and ouputs a csv file
// of addresses in the format expected for the lab
function genSampleAddresses(inputfile, outputfile, month, year, randomBad, customerId, orderId){
    randIndices = generateRandomizationArray(randomBad);
    //console.log('randIndices is ' + randIndices);
    custID = customerId;
    ordID = orderId;

    let src_file = fs.createReadStream(inputfile); // input stream
 
    let parser = parse( // creates the csv parser with options set
        {
            columns: true,
            trim: true, 
            skip_lines_with_error: true,
        });


    let transformer = transform((data)=>transformAddressCsv(data, month, year, randIndices)); // calls function to transform the data

    var stringifier = stringify( // creates string output with options
        { 
            header: false,
        });

    //let writeStream = fs.createWriteStream(outputfile); // output stream
    let writeStream = rfs(outputfile, {
        path: 'outputs',
        size:'2G',
        rotate: 1,
        compress: true,
    });

    // creates the pipeline and executes
    src_file.pipe(parser).pipe(transformer).pipe(stringifier).pipe(writeStream);
}

function findBestWarehouse(deliveryLat, deliveryLon) {

   var bestDistanceMeasured = 1000000;
   var returnI = 0;
   for (i = 0; i < 3; i++) {

      var deliveryLoc = {
  	lat: deliveryLat,
  	lon: deliveryLon
      };
      var warehouseLoc = {
  	lat: warehouse_locations_lat[i],
  	lon: warehouse_locations_lon[i] 
      };
      var distanceMeasured = geomeasure.between(deliveryLoc, warehouseLoc);

      //console.log('for i ' + i + ' distance measured is ' + distanceMeasured.human_readable());
      if (distanceMeasured < bestDistanceMeasured) {
         //console.log('new best is now ' + i);
         returnI = i;
         bestDistanceMeasured = distanceMeasured;
      }
   }
   return returnI;
}

function findWorstWarehouse(deliveryLat, deliveryLon) {

   var worstDistanceMeasured = 0;
   var returnI = 0;
   for (i = 0; i < 3; i++) {
      
      var deliveryLoc = {
        lat: deliveryLat,
        lon: deliveryLon
      };
      var warehouseLoc = {
        lat: warehouse_locations_lat[i],
        lon: warehouse_locations_lon[i]
      };
      var distanceMeasured = geomeasure.between(deliveryLoc, warehouseLoc);
      
      //console.log('' + distanceMeasured.human_readable());
      if (distanceMeasured > worstDistanceMeasured) {
         worstDistanceMeasured = distanceMeasured;
         returnI = i;
      }
   }
   return returnI;
}

// creates the expected output format and substitutes in defaults from the defined constant
function transformAddressCsv(data, yr, mn, randomArray){
    let minimizedData = null;

    if (data.street != "" ) // only create entries for lines including address 
	{
	var rn = require('random-number');
        var orderDT = randomdate({
                        year: yr,
                        month: mn
                });
        var goodOrBad = notReallyRandom(randomArray);
        //console.log('good or bad data is ' + goodOrBad);
	// if value = 2, this is an on-time (or early) delivery
        var amtTimeAdd = 0;
        var whAdd = '123 some street';
        var whCity = 'Las Vegas';
        var whSt = 'NV';
        var whZip = '11111';
        var whLat = '0.0';
        var whLon = '0.0';
   	var trckId = 1;

        whbest = findBestWarehouse(data.lat, data.lon);
        whworst = findWorstWarehouse(data.lat, data.lon);

        if (goodOrBad == 1) {
       		amtToAddToDelivery = rn.generator({
					min: 2, max:1080, integer: true
		}); 
		amtTimeAdd = amtToAddToDelivery();
		//console.log('bad delivery, amount to add is ' + amtTimeAdd);
		whAdd = warehouse_locations_street[whworst];
		whCity = warehouse_locations_city[whworst];
		whSt = warehouse_locations_state[whworst];
		whZip = warehouse_locations_zip[whworst];
		whLat = warehouse_locations_lat[whworst];
		whLon = warehouse_locations_lon[whworst];
		trckId = truck_id[whworst]; 
               
	}
	else if (goodOrBad == 2) {
		amtToAddToDelivery = rn.generator({
					min: -180, max: 0, integer: true
		})
		amtTimeAdd = amtToAddToDelivery();
		//console.log('good delivery, amount to add is ' + amtTimeAdd);
                whAdd = warehouse_locations_street[whbest];
                whCity = warehouse_locations_city[whbest];
                whSt = warehouse_locations_state[whbest];
                whZip = warehouse_locations_zip[whbest];
                whLat = warehouse_locations_lat[whbest];
                whLon = warehouse_locations_lon[whbest];
		trckId = truck_id[whbest];
	}
        var promisedDT = new Date(orderDT);
        let promDT = datetime.addDays(promisedDT, 1);
        let actualDT = datetime.addMinutes(promDT, amtTimeAdd);
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
	    orderid: ordID++,
            customer_id: custID++,
            price: prices[randomPriceInt],
            delivery_zone: trckId,
            warehouse_address: whAdd,
            warehouse_city: whCity,
            warehouse_state: whSt,
            warehouse_zip: whZip,
            warehouse_lat: whLat,
            warehouse_lon: whLon,
            product: conf.product_name,
	    productdelivered: conf.product_delivered,
            promiseddate: datetime.format(promDT,
                'YYYY-MM-DDTHH:mm'),
	    actualdate: datetime.format(actualDT,
		'YYYY-MM-DDTHH:mm')
	    }
         //console.log('street is not NULL, return good data');
	 return minimizedData;

        }
    else {
      //console.log('street is NULL, return null');
      return null;
    }
}

// runs a test for local file
// argv2 = input file
// argv3 = output file
// argv4 = month
// argv5 = year
// argv6 = random percentage of "bad" data 
// argv7 = customer ID to start with
// argv8 = order ID to start with
	genSampleAddresses(process.argv[2], process.argv[3], process.argv[4], process.argv[5], process.argv[6], process.argv[7], process.argv[8]);
