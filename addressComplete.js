var stream = require('stream');
var util = require('util');


const googleMapsClient = require('@google/maps').createClient({
    key: '',
    Promise: Promise
  });

var Transform = stream.Transform;

function Upper(options) {
  // allow use without new
  if (!(this instanceof Upper)) {
    return new Upper(options);
  }

  // init Transform
  Transform.call(this, options);
}
util.inherits(Upper, Transform);

Upper.prototype._transform = function (chunk, enc, cb) {
    console.log(chunk.toString());
    googleMapsClient.placesAutoComplete({
        input: chunk.toString(),//partialAddress.address,
        sessiontoken: "aaa"
        //location: partialAddress.lat +','+ partialAddress.lon
    }).asPromise().then((response)=>{ console.log(response);
        return response});
    
    //var upperChunk = chunk.toString().toUpperCase();
    //this.push(upperChunk);
    cb();
};


// try it out
/*var upper = new Upper();
upper.pipe(process.stdout); // output to stdout
upper.write('hello world\n'); // input line 1
upper.write('another line\n');  // input line 2
upper.end();  // finish*/

googleMapsClient.placesAutoComplete({
    input: "1387 N Church Ct, Bellbrook",
    sessiontoken: "aaa"
}).asPromise().then((response)=>{ console.log("done")});