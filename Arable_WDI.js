var fs = require('fs');
var stream = require('stream');

//Creating OutStreams for each json file
var outStream1 = fs.createWriteStream("json/indiaArable.json");
var outStream2 = fs.createWriteStream("json/indiaHectaresPP.json");
var outStream3 = fs.createWriteStream("json/indiaHectares.json");
var outStream4 = fs.createWriteStream("json/africaArable.json");
var outStream5 = fs.createWriteStream("json/aggregate.json");

//Creating Instream and Reading Files
var rl = require('readline').createInterface({
  input: require('fs').createReadStream('../Data/WDIData.csv')
});
var indiaArable = new Array();
var indiaHectaresPP = new Array();
var indHectares = new Array();
var africaArable= new Array();

// To Read JSON file of Continents
var continentArray;
var r2 = require('readline').createInterface({
  input: require('fs').createReadStream('../Data/continent.json')
});

global.continents = [];
r2.on('line', function(line) {
  global.continents.push(line);
});

r2.on('close', function(){
  var continents = [];
  // console.log(global.continents.length);
  for(var i=1;i<global.continents.length -1;i++){
    var continenetObj = {};
    global.continents[i] = global.continents[i].replace(/"/g,"");
    global.continents[i] = global.continents[i].split(":");
    continenetObj['Country'] = global.continents[i][0].trim();
    continenetObj['Continent'] = global.continents[i][1].trim();
    continenetObj['Continent'] = continenetObj['Continent'].replace(/,\s*$/, "");
    continents.push(continenetObj);
  }
  continentArray = concatValueForCommonKeys(continents, 'Continent');      
});

//for continents.json file
function concatValueForCommonKeys(array, commonObject){
  var tempObj = {};
  var uniqueKeyArray = [];
  for(var arrayItem in array){
    var obj1 = array[arrayItem];
    var obj2 = tempObj[obj1[commonObject]];

    if(!obj2){
      uniqueKeyArray.push(obj2 = tempObj[obj1[commonObject]] = {});
    }
    for(var k in obj1) {
      obj2[k] = (k === commonObject) ? obj1[commonObject] : (obj2[k] || '')+","+(obj1[k]);
    }
  }
  for(var i=0;i<uniqueKeyArray.length;i++){
    uniqueKeyArray[i].Country = uniqueKeyArray[i].Country.substr(1);    
    uniqueKeyArray[i].Country = uniqueKeyArray[i].Country.split(',');
    
  }
  return uniqueKeyArray;
}


//Reading the Main Data
global.myarray = [];
var aggregate = [];
rl.on('line', function(line) {
  global.myarray.push(line);
});

rl.on('close', function(){
  var heading, line;

  for(var i=0;i<global.myarray.length;i++){    
    line = global.myarray[i].replace(/"/g,"");
    line = line.split(",");
    if(i == 0){
      heading = line
    }
    
    if(line[0] == "India" && line[2] == "Arable land (% of land area)"){     
      for(var j=4;j<line.length - 2;j++){
        var obj= {};
        obj["YEAR"] =  heading[j];
        obj["Percentage Value of Land Area"] =  parseFloat(line[j]);
        indiaArable.push(obj);
      }      
    }else if(line[0] == "India" && line[2] == "Arable land (hectares per person)"){
      for(var j=4;j<line.length - 2;j++){
        var obj= {};
        obj["YEAR"] =  heading[j];
        obj["Arable land (hectares per person) Value"] =  parseFloat(line[j]);
        indiaHectaresPP.push(obj);
      }
    }else if(line[0] == "India" && line[2] == "Arable land (hectares)"){
      for(var j=4;j<line.length - 2;j++){
        var obj= {};
        obj["YEAR"] =  heading[j];
        obj["Arable land (hectares) Value"] =  parseFloat(line[j]);
        indHectares.push(obj);
      }
    }else if((line[0] == "Sub-Saharan Africa" || line[0] == "Sub-Saharan Africa (excluding high income)")  && line[2] == "Arable land (% of land area)"){
      for(var j=4;j<line.length - 2;j++){
        var obj= {};
        obj["YEAR"] =  heading[j];
        obj["African Arable land (% of land area) Value"] =  parseFloat(line[j]);
        africaArable.push(obj);
      }
    }else if(line[2] == "Arable land (hectares)"){
      for(var k=0;k<continentArray.length;k++){ 
        for(var l=0;l<continentArray[k].Country.length;l++){
          if(line[0] == continentArray[k].Country[l]){     
            for(var j=4;j<line.length -1;j++){
              var obj= {};
              obj["Year"] =  heading[j]
              obj[continentArray[k].Continent] = parseFloat(line[j]); 
              aggregate.push(obj);           
            }  
          }
        }   
      }
    }
  }

var aggregateJSON = addValueForCommonKeys(aggregate, 'Year');

  //Creating all json files
  outStream1.write(JSON.stringify(indiaArable));
  outStream2.write(JSON.stringify(indiaHectaresPP));
  outStream3.write(JSON.stringify(indHectares));
  outStream4.write(JSON.stringify(africaArable));
  outStream5.write(JSON.stringify(aggregateJSON));


});

// for aggregation purpose
function addValueForCommonKeys(array, commonObject){
  var tempObj = {};
  var uniqueKeyArray = [];
  for(var arrayItem in array){
    var obj1 = array[arrayItem];
    var obj2 = tempObj[obj1[commonObject]];

    if(!obj2){
      uniqueKeyArray.push(obj2 = tempObj[obj1[commonObject]] = {});
    }
    for(var k in obj1) 
      obj2[k] = k === commonObject ? obj1[commonObject] : parseInt(obj2[k]||0)+parseInt(obj1[k]);
  }
  return uniqueKeyArray ;
}
