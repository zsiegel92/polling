const fs = require('fs');
const turf = require('@turf/turf');
const { Parser } = require('json2csv');
const random = require("random")
// const https = require('https');
// const request = require('request');
const superagent = require('superagent');
const demoData = require('./demographic_data_tables_lookup.js');

// PER-RUN VARIABLES
var WAIT_TIME_BETWEEN_SEARCHES = 0.3;
var max_points_to_search_per_city = 26000;
// var interested_states = ['AZ']; //EXTRACTED_STATE
var interested_cities = ['Dover, DE Metro Area']; //NAMELSAD
var cellSize = 0.295; // Grid spacing - Kilometers
var noiseSD = 0;
var max_cities_to_process = 1;


var api_key = 'redacted';    // GOOGLE API - Auyon/Nic
var api_key2 = 'redacted'; // Google API - Zach
var census_api_key = 'redacted' // CENSUS API
var geocoding_api_key = "redacted"; //GEOCODING API
var geocoding_api_key2 = 'redacted'; // Geocoding API - Zach
var cities_with_states_path = "./cities_with_states.json";
var cities_with_states_and_extracted_states_path = "./cities_with_extracted_states.json";
var NormGen = random.normal(mu = 0, sigma = noiseSD);

var demographic_data_tables_lookup = demoData.demographic_data_tables_lookup;
var census_api_query_url = demoData.census_api_query_url;
var state_abbrevs = demoData.state_abbrevs;



var cities;
var cities_with_multiple = [];
var current_election_ids = [];
var current_election_names = [];
var cities_with_elections = [];
// var interested_metro_areas =
var all_polling_locations = {};
var grids = {};
var grid_info = {};
var grid = [];
var current_search_point = 0;
var tot = 0;

main();
grid = grids[Object.keys(grids)[0]];


async function main() {
    // let rawdata = fs.readFileSync('tl_2017_us_cbsa/metro_shapes.json');
    let rawdata = fs.readFileSync('tl_2019_us_cbsa/metro_shapes.json');
    let featureCollection = JSON.parse(rawdata);
    var numberMetropolitanAreas = Object.keys(featureCollection.features).length;
    cities = featureCollection.features;

    // READ UPDATED CITIES
    // read_cities_with_states();
    // await read_cities_with_extracted_states();
    await extract_all_states();


    // NOTE MULTIPLE-ELECTION METRO AREAS (MULTIPLE STATES)

    await track_multiple();



    await get_current_elections();


    await processCitiesWithElections();

    grid = grids[Object.keys(grids)[0]];



}



function save_grid_csv(grid, filename) {

    var fields = [];
    var props_only = [];
    for (let p of grid) {
        props_only.push(p.properties);
        for (let k of Object.keys(p.properties)) {
            if (!fields.includes(k)) {
                fields.push(k);
            }
        }
    }

    const storeData = (data, path) => {
        try {
            fs.writeFileSync(path, data)
        } catch (err) {
            console.error(err)
        }
    }

    const parser = new Parser({ fields });
    const csv = parser.parse(props_only);
    // console.log(csv);

    storeData(csv, filename);
}

function save_polling_locations_csv(election_id, filename) {
    var polling_locations = all_polling_locations[election_id];
    var fields = [];
    var props_only = [];
    for (let p of polling_locations) {
        p.properties.lat = p.geometry.coordinates[1];
        p.properties.lng = p.geometry.coordinates[0];
        props_only.push(p.properties);
        for (let k of Object.keys(p.properties)) {
            if (!fields.includes(k)) {
                fields.push(k);
            }
        }
    }

    const storeData = (data, path) => {
        try {
            fs.writeFileSync(path, data)
        } catch (err) {
            console.error(err)
        }
    }

    // try {
    const parser = new Parser({ fields });
    const csv = parser.parse(props_only);
    // console.log(csv);
    // } catch (err) {
    //   console.log("ERROR ERROR");
    //   console.log(err);
    // }
    storeData(csv, filename);

}

function generate_run_identifier() {
    var d = new Date();
    var month = d.getMonth() + 1;
    var day = d.getUTCDate();
    var year = d.getFullYear();
    var hours = d.getHours();
    var date = month + "_" + day + "_" + year + "_" + hours + "hrs_" + noiseSD + "noise_";
    return date;
}

function generate_filename(city, election_id, election_name) {
    var date_id = generate_run_identifier();
    grid_info[city.properties.NAMELSAD] = { 'election_id': election_id, 'election_name': election_name, 'date': date_id };
    var filename = city.properties.NAMELSAD + "_" + election_name + "_" + election_id + "_" + cellSize + "km_" + date_id + "POINTS.csv";
    return "results/" + filename;
}

function generate_polling_location_filename(city, election_id, election_name) {
    var date_id = generate_run_identifier();
    grid_info[city.properties.NAMELSAD] = { 'election_id': election_id, 'election_name': election_name, 'date': date_id };
    var filename = city.properties.NAMELSAD + "_" + election_name + "_" + election_id + "_" + cellSize + "km_" + date_id + "POLLING.csv";
    return "results/" + filename;
}

async function processCityWithElection(city) {
    var name = city.properties.NAMELSAD;
    var election_id = city.properties.CURRENT_ELECTION_ID;
    var election_name = city.properties.CURRENT_ELECTION_NAME;

    if (!(election_id in all_polling_locations)) {
        all_polling_locations[election_id] = [];
    }
    console.log(city.properties.NAME);
    console.log(election_name);
    console.log("    ");
    var thePoints = generate_search_grid(city);
    console.log("CREATED GRID WITH " + thePoints.length + " POINTS!");
    tot = thePoints.length;
    var filename = generate_filename(city, election_id, election_name);
    var polling_location_filename = generate_polling_location_filename(city, election_id, election_name);
    await search_points(thePoints, election_id, filename, polling_location_filename);
    grids[city.properties.NAMELSAD] = thePoints;

}

async function processCitiesWithElections() {
    var i = 0;
    for (let city of cities_with_elections) {
        if (i < max_cities_to_process) {
            processCityWithElection(city)
        }
        i++;
    }
}


async function search_points(points, election_id, filename, polling_location_filename) {
    current_search_point = 0;
    var count = 0;
    for (let p of points) {
        if (count < max_points_to_search_per_city) {
            await processPoint(p, election_id, filename, polling_location_filename, points).then(function (result) {
                console.log("Processed point!");
            })
        }
        count++;
    };
};



function printCitiesWithElections() {
    for (let the_city of cities_with_elections) {
        console.log(the_city.properties.NAME);
        console.log(the_city.properties.EXTRACTED_STATE);
        console.log(the_city.properties.CURRENT_ELECTION_NAME);
    }
}

function printCitiesWithMultiple() {
    for (let index of cities_with_multiple) {
        var the_city = cities[index];
        console.log(the_city.properties.NAME);
        console.log(the_city.properties.EXTRACTED_STATE);
        // console.log(the_city.properties.CURRENT_ELECTION_NAME);
    }
}
function uniquifyPollingLocations() {
    for (var key in all_polling_locations) {
        var l = all_polling_locations[key];
        var string_l = l.map(x => JSON.stringify(x));
        var unique = [];
        var string_unique = [];
        for (var i = 0; i < string_l.length; i++) {
            if (string_unique.includes(string_l[i])) {
                // ALREADY HAVE POLLING LOCATION
            }
            else {
                string_unique.push(string_l[i]);
                unique.push(l[i]);
            }
        }
        all_polling_locations[key] = unique;
    }
}



function track_multiple() {
    cities.forEach((city, index) => {
        if (city.properties.EXTRACTED_MULTIPLE_STATES.length > 0) {
            cities_with_multiple.push(index);
            // console.log(city.properties.EXTRACTED_MULTIPLE_STATES);
        }
    });
}



function attach_elections() {

    cities.forEach((city, index) => {
        if (city.properties.EXTRACTED_MULTIPLE_STATES.length == 0) {
            var abbrev = city.properties.EXTRACTED_STATE;
            var has_state = false;
            var has_current_election = false;
            for (let pair of state_abbrevs) {
                if (pair[1] == abbrev) {
                    var state_fullname = pair[0];
                    city.properties.LONG_STATE_NAME = state_fullname;
                    has_state = true;;
                }
            }
            if (has_state == true) {
                current_election_names.forEach((name, election_index) => {
                    if (name.includes(state_fullname)) {
                        has_current_election = true;
                        city.properties.CURRENT_ELECTION_NAME = name;
                        city.properties.CURRENT_ELECTION_ID = current_election_ids[election_index];
                    }
                    else {
                        //DO NOTHING
                    }
                });
                city.properties.HAS_CURRENT_ELECTION = has_current_election;
                if (city.properties.HAS_CURRENT_ELECTION == true) {
                    if (!city.properties.NAMELSAD.includes("Micro")) {
                        if (interested_cities.length > 0) {
                            if (interested_cities.includes(city.properties.NAMELSAD)) {
                                cities_with_elections.push(city);
                            }
                        }
                        else {
                            cities_with_elections.push(city);
                        }
                    }
                }

            }
            else {
                // console.log("CITY HAS NO VALID STATE!");
                // console.log(index);
                // console.log(city);
            }
            // current_election_names
            // current_election_ids
            // state_abbrevs
        }
        else {
            //DO NOTHING FOR THESE METRO AREAS FOR NOW
        }
    });
}

function get_all_states() {
    cities.forEach((city) => {
        get_state(city);
    });

    const storeData = (data, path) => {
        try {
            fs.writeFileSync(path, JSON.stringify(data))
        } catch (err) {
            console.error(err)
        }
    }
    storeData(cities, cities_with_states_path);
}
// get_all_states();

function get_state(city) {
    var lat = city.properties.INTPTLAT;
    var lon = city.properties.INTPTLON;

    url = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + lat + "," + lon + "&key=" + geocoding_api_key;
    superagent.get(url).then(function (response) {
        var data = JSON.parse(response.text);
        var short_name = "";
        var long_name = "";
        if (!('results' in data)) {
            console.log('Error retrieving reverse geocoding for city');
            console.log(city);
        } else {
            var address_components = data.results[0].address_components;
            address_components.forEach(function (component) {
                if (component.types.includes("administrative_area_level_1")) {
                    long_name = component.long_name;
                    short_name = component.short_name;
                    city.properties.STATELONG = long_name;
                    city.properties.STATESHORT = short_name;
                }
            });
        }

    }).catch(function (err) {
        console.log('Error retrieving reverse geocoding for city');
        console.log(city);
    });
}

function read_cities_with_states() {
    const loadData = (path) => {
        try {
            return fs.readFileSync(path, 'utf8')
        } catch (err) {
            console.error(err)
            return false
        }
    }
    cities = JSON.parse(loadData(cities_with_states_path));
}

function extract_all_states() {
    //TODO : get two-character state abbrev after comma in "NAMELSAD" or in "NAME" field of city.properties
    for (let city of cities) {
        extract_state(city);
    }
    // cities.forEach((city) => {
    //      extract_state(city);
    //  });
    // var saves = false;
    // if (saves){
    //  const storeData = (data, path) => {
    //    try {
    //      fs.writeFileSync(path, JSON.stringify(data))
    //    } catch (err) {
    //      console.error(err)
    //    }
    //  }
    //  storeData(cities,cities_with_states_and_extracted_states_path);
    // }
}
function extract_state(city) {
    var states = city.properties.NAME.split(", ").reduceRight(a => a); //gets last element
    var statesList = states.split("-");
    if (statesList.length == 1) {
        // city.properties.STATELONG = long_name;
        city.properties.EXTRACTED_STATE = states;
        city.properties.EXTRACTED_MULTIPLE_STATES = [];
    }
    else {
        city.properties.EXTRACTED_STATE = "multiple";
        city.properties.EXTRACTED_MULTIPLE_STATES = statesList;
    }
}

function read_cities_with_extracted_states() {
    const loadData = (path) => {
        try {
            return fs.readFileSync(path, 'utf8')
        } catch (err) {
            console.error(err)
            return false
        }
    }
    cities = JSON.parse(loadData(cities_with_states_and_extracted_states_path));
}

function generate_search_grid(city) {
    var features_within = [];
    var bbox = turf.bbox(city);
    var grid = turf.pointGrid(bbox, cellSize);//, {units: 'kilometers'}

    if (noiseSD > 0) {
        for (let p of grid.features) {
            // https://turfjs.org/docs/#rhumbDestination
            var bearing = Math.random() * 360 - 180;
            var distance = Math.abs(NormGen());
            var options = { units: 'kilometers' };
            var destination = turf.rhumbDestination(p, distance, bearing, options);
            p.geometry = destination.geometry;
        }
    }
    grid.features.forEach(function (p) {
        if (turf.booleanContains(city, p)) {
            features_within.push(p);
        }
    });

    if (features_within.length == 0) {
        console.log('Grid size too large, no points within boundary.');
    }
    return features_within;
}






async function get_current_elections() {
    var url = 'https://www.googleapis.com/civicinfo/v2/elections?key=' + api_key;
    await superagent.get(url).then(function (response) {
        var data = JSON.parse(response.text);
        var num_elections = data.elections.length;
        var num_processed = 0;
        console.log("NUMBER OF ELECTIONS:");
        console.log(num_elections);

        // console.log(data);
        if (!('elections' in data)) {
            console.log('Error retrieving available elections from Civics API. Please make sure your API key is valid and the Civics API is enabled for that key!')
        } else {
            data.elections.forEach(function (election) {
                current_election_ids.push(election.id);
                current_election_names.push(election.name);
                num_processed++;
                if (num_processed == num_elections) {
                    console.log("PUSHED ALL ELECTIONS");
                    attach_elections();
                }
            });
        }
    }).catch(function (err) {
        console.log('Error retrieving available elections from Civics API. Please make sure your API key is valid and the Civics API is enabled for that key!');
    });
}



async function processPoint(p, election_id, filename, polling_location_filename, points) {

    return new Promise(resolve => {
        console.log("PROCESSING A POINT");
        var coords = turf.getCoords(p)
        // set default attribute values
        var lat = coords[1]; //LISTED AS [ lon, lat ]
        var lng = coords[0]; //LISTED AS [ lon, lat ]
        p.properties['latitude'] = lat;
        p.properties['longitude'] = lng;

        // include all datapoints in demographic data
        for (table_id in demographic_data_tables_lookup) {
            var table_name = demographic_data_tables_lookup[table_id];
            p.properties[table_name] = '';
        }

        // polling station info
        p.properties['pollingLocationLatitude'] = '';
        p.properties['pollingLocationLongitude'] = '';
        p.properties['pollingLocationName'] = '';
        p.properties['pollingLocationAddress'] = '';
        p.properties['pollingLocationCity'] = '';
        p.properties['pollingLocationState'] = '';
        p.properties['pollingLocationZip'] = '';
        p.properties['pollingLocationNotes'] = '';
        p.properties['pollingLocationHours'] = '';
        p.properties['pollingLocationHours'] = '';

        // travel time / distance info
        p.properties['distance'] = '';
        p.properties['duration'] = '';
        p.properties['routePolyline'] = '';

        // add log message
        console.log(current_search_point + 1 + '/' + tot + ' ' + lat.toFixed(6) + ', ' + lng.toFixed(6));

        // query our google cloud function to get census data for a given lat lng
        url = "https://us-central1-corded-palisade-136523.cloudfunctions.net/add_census_data?census_api_key=" + census_api_key + "&lat=" + lat + "&lng=" + lng
        url += '&noReturn=true' //If census checkbox not checked (do not get census info)


        setTimeout(function () {

            // API CALL 1 (CENSUS)
            superagent.get(url).then(function (response) {
                var data = JSON.parse(response.text);
                if (data['status'] != undefined) {
                    // this means an error occured

                } else {
                    // all good, add data to point
                    for (prop in data) {
                        p.properties[prop] = data[prop]
                    }

                    // now get address
                    var address = '';

                    var latLng = { 'lat': lat, 'lng': lng };

                    url = "https://us-central1-corded-palisade-136523.cloudfunctions.net/geocode?api_key=" + api_key + "&lat=" + lat + "&lng=" + lng;

                    // API CALL 2 (REVERSE GEOCODE)
                    superagent.get(url).then(function (response) {
                        var geocode_response = JSON.parse(response.text);
                        console.log("LOGGING REVERSE GEOCODE RESPONSE");
                        var status = geocode_response['status'];

                        if (status === 'OK') {
                            result = geocode_response['result'];
                            address = result['formatted_address'];

                            // if we got to here, we have an address. Now find polling stations
                            console.log(' -> got address');
                            pollingLocationName = '';
                            pollingLocationAddress = '';
                            pollingLocationCity = '';
                            pollingLocationState = '';
                            pollingLocationZip = '';
                            pollingLocationNotes = '';
                            pollingLocationHours = '';

                            var url = 'https://www.googleapis.com/civicinfo/v2/voterinfo?address=' + address + '&electionId=' + election_id + '&key=' + api_key

                            // API CALL 3 (CIVIC API)
                            superagent.get(url).then(function (response) {
                                var data = JSON.parse(response.text);
                                console.log("GOT CIVIC API RESPONSE");
                                if (!('error' in data)) {
                                    if ('pollingLocations' in data) {
                                        polling_locations = data['pollingLocations']
                                        pl = polling_locations[0]
                                        pollingLocationName = pl['address']['locationName']
                                        pollingLocationAddress = pl['address']['line1']
                                        pollingLocationCity = pl['address']['city']
                                        pollingLocationState = pl['address']['state']
                                        pollingLocationZip = pl['address']['zip']
                                        if ('notes' in pl) { pollingLocationNotes = pl['notes'] }
                                        if ('pollingHours' in pl) { pollingLocationNotes = pl['pollingHours'] }


                                        p.properties['pollingLocationName'] = pollingLocationName;
                                        p.properties['pollingLocationAddress'] = pollingLocationAddress;
                                        p.properties['pollingLocationCity'] = pollingLocationCity;
                                        p.properties['pollingLocationState'] = pollingLocationState;
                                        p.properties['pollingLocationZip'] = pollingLocationZip;
                                        p.properties['pollingLocationNotes'] = pollingLocationNotes;
                                        p.properties['pollingLocationHours'] = pollingLocationHours;





                                        // if we are here, we got a polling location for the address, lat lng of polling station from address
                                        console.log(' -> got a polling station')

                                        // if ($('#select-distance-type').val() == 'straight') {
                                        if (true) {

                                            var pollingStationLat = '';
                                            var pollingStationLng = '';


                                            var address = pollingLocationAddress + ' ' + pollingLocationCity + ' ' + pollingLocationState


                                            url = "https://us-central1-corded-palisade-136523.cloudfunctions.net/geocode?api_key=" + api_key + "&address=" + address;
                                            // API CALL 4 (GEOCODE POLLING PLACE)
                                            superagent.get(url).then(function (response) {
                                                var geocode_response = JSON.parse(response.text);
                                                var status = geocode_response['status'];

                                                if (status == 'OK') {
                                                    result = geocode_response['result'];
                                                    pollingStationLat = parseFloat(result['lat']);
                                                    pollingStationLng = parseFloat(result['lng']);

                                                    // RECALL - THIS HAS BEEN CALLED:
                                                    // var coords = turf.getCoords(p)
                                                    // var lat = coords[1]; //LISTED AS [ lon, lat ]
                                                    // var lng = coords[0]; //LISTED AS [ lon, lat ]

                                                    var distance = turf.distance(turf.point([lng, lat]), turf.point([pollingStationLng, pollingStationLat]), { units: 'meters' })
                                                    // console.log("DISTANCE WORKED FINE");
                                                    p.properties['distance'] = distance
                                                    p.properties['duration'] = ''
                                                    p.properties['routePolyline'] = ''
                                                    p.properties['pollingLocationLatitude'] = pollingStationLat;
                                                    p.properties['pollingLocationLongitude'] = pollingStationLng;

                                                    // add icon to map
                                                    var feature = {
                                                        "type": "Feature",
                                                        "geometry": {
                                                            "type": "Point",
                                                            "coordinates": [pollingStationLng, pollingStationLat]
                                                        },
                                                        "properties": {
                                                            'name': pollingLocationName,
                                                            'address': pollingLocationAddress,
                                                            'city': pollingLocationState,
                                                            'state': pollingLocationState,
                                                            'zip': pollingLocationZip,
                                                            'notes': pollingLocationNotes,
                                                            'hours': pollingLocationHours,
                                                        }
                                                    }

                                                    all_polling_locations[election_id].push(feature);

                                                    // polling_stations_layer.addData(feature);
                                                    resolve('success');
                                                } else {
                                                    console.log(' -> error geocoding polling station')
                                                    resolve('error');
                                                }
                                            }).catch(function (err) {
                                                console.log('Error with API call 4 (GEOCODING POLLING LOCATION) (from outer)');
                                                console.log(err);
                                                resolve('error');
                                            });


                                        }

                                    } else {
                                        console.log(' -> no polling station found')
                                        resolve('no results');
                                    }
                                } else {
                                    resolve('error');
                                }
                            }).catch(function (err) {
                                console.log('Error with API call 3 (CIVIC API) (from outer)');
                                console.log(err);
                                resolve('error');
                            });
                        } else {
                            console.log(' -> error reverse geocoding, status = ' + status)
                            resolve('error');
                        }
                    }).catch(function (err) {
                        console.log('Error with API call 2 (REVERSE GEOCODE) (from outer)');
                        console.log(err);
                        resolve('error');
                    });

                }
            }).catch(function (err) {
                console.log('Error with API call 1 (CENSUS) (from outer)');
                resolve('error');
            });
        }, WAIT_TIME_BETWEEN_SEARCHES * 1);


        // remember to increment point counter
        current_search_point++;
        console.log("CURRENT SEARCH POINT: " + current_search_point + "/" + Math.min(tot, max_points_to_search_per_city));
        if (current_search_point == Math.min(tot, max_points_to_search_per_city)) {
            uniquifyPollingLocations();
            save_grid_csv(points, filename);
            save_polling_locations_csv(election_id, polling_location_filename);
        }
    })
        .catch((error) => {
            console.log("ERROR PROCESSING A POINT");
            console.log(error);
            resolve('error');
        });
}



// function addSearchPoint(e) {
//     var feature = {
//       "type": "Feature",
//       "geometry": {
//         "type": "Point",
//         "coordinates": [e.latlng.lng, e.latlng.lat]
//       },
//       "properties": {
//       }
//     }
//     search_points_layer.addData(feature);
//     var l = search_points_layer.getLayers()[search_points_layer.getLayers().length-1]
//     l.enableEdit();
//     l.on('click', deletePoint);
// }
