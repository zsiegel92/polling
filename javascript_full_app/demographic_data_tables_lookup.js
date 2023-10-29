var demographic_data_tables_lookup = {
	"B01003_001E":"Total Pop",
	"B19013_001E":"Median Income",
	"B01001_002E":"Male Pop",
	"B01001_003E":"Male Pop - Under 5 years",
	"B01001_004E":"Male Pop - 5-9 years",
	"B01001_005E":"Male Pop - 10-14 years",
	"B01001_006E":"Male Pop - 15-17 years",
	"B01001_007E":"Male Pop - 18-19 years",
	"B01001_008E":"Male Pop - 20 years",
	"B01001_009E":"Male Pop - 21 years",
	"B01001_010E":"Male Pop - 22-24 years",
	"B01001_011E":"Male Pop - 25-29 years",
	"B01001_012E":"Male Pop - 30-34 years",
	"B01001_013E":"Male Pop - 35-39 years",
	"B01001_014E":"Male Pop - 40-44 years",
	"B01001_015E":"Male Pop - 45-49 years",
	"B01001_016E":"Male Pop - 50-54 years",
	"B01001_017E":"Male Pop - 55-59 years",
	"B01001_018E":"Male Pop - 60-61 years",
	"B01001_019E":"Male Pop - 62-64 years",
	"B01001_020E":"Male Pop - 65-66 years",
	"B01001_021E":"Male Pop - 67-69 years",
	"B01001_022E":"Male Pop - 70-74 years",
	"B01001_023E":"Male Pop - 75-79 years",
	"B01001_024E":"Male Pop - 80-84 years",
	"B01001_025E":"Male Pop - 85 years and over",
	"B01001_026E":"Female Pop",
	"B01001_027E":"Female Pop - Under 5 years",
	"B01001_028E":"Female Pop - 5-9 years",
	"B01001_029E":"Female Pop - 10-14 years",
	"B01001_030E":"Female Pop - 15-17 years",
	"B01001_031E":"Female Pop - 18-19 years",
	"B01001_032E":"Female Pop - 20 years",
	"B01001_033E":"Female Pop - 21 years",
	"B01001_034E":"Female Pop - 22-24 years",
	"B01001_035E":"Female Pop - 25-29 years",
	"B01001_036E":"Female Pop - 30-34 years",
	"B01001_037E":"Female Pop - 35-39 years",
	"B01001_038E":"Female Pop - 40-44 years",
	"B01001_039E":"Female Pop - 45-49 years",
	"B01001_040E":"Female Pop - 50-54 years",
	"B01001_041E":"Female Pop - 55-59 years",
	"B01001_042E":"Female Pop - 60-61 years",
	"B01001_043E":"Female Pop - 62-64 years",
	"B01001_044E":"Female Pop - 65-66 years",
	"B01001_045E":"Female Pop - 67-69 years",
	"B01001_046E":"Female Pop - 70-74 years",
	"B01001_047E":"Female Pop - 75-79 years",
	"B01001_048E":"Female Pop - 80-84 years",
	"B01001_049E":"Female Pop - 85 years and over",
	"B02001_002E":"White Pop",
	"B02001_003E":"Black Pop",
	"B02001_004E":"American Indian Pop",
	"B02001_005E":"Asian Pop",
	"B02001_006E":"Native Hawaiian Pop",
	"B02001_007E":"Other Pop",
	"B03001_002E":"Not Hispanic Pop",
	"B03001_003E":"Hispanic Pop",
	"B08014_002E":"No vehicles",
	"B08014_003E":"1 vehicle",
	"B08014_004E":"2 vehicles",
	"B08014_005E":"3 vehicles",
	"B08014_006E":"4 vehicles",
	"B08014_007E":"5 or more vehicles",
	"B08015_001E":"Aggregate vehicles used in commuting",
	"B08006_002E":"Work commute - Driving",
	"B08006_008E":"Work commute - Transit",
	"B08006_014E":"Work commute - Bike",
	"B08006_015E":"Work commute - Walk",
	"B08006_016E":"Work commute - Other",
	"B08006_017E":"Work commute - Work from home",
	"B15003_017E":"High school diploma",
	"B15003_018E":"GED",
	"B15003_019E":"Some college less than 1 year",
	"B15003_020E":"Some college more than 1 year",
	"B15003_021E":"Associates degree",
	"B15003_022E":"Bachelor's degree",
	"B15003_023E":"Master's degree",
	"B15003_024E":"Professional school degree",
	"B15003_025E":"Doctorate degree"
}

var groups = [
	"B01001",
]

var census_api_query_url = "https://api.census.gov/data/2017/acs/acs5?&get="

var get_items = [];

groups.forEach(function(group) {
	get_items.push("group("+group+")")
});

Object.keys(demographic_data_tables_lookup).forEach(function(table_id) {

	var inGroup = false;
	groups.forEach(function(group) {
		if (table_id.startsWith(group)) {
			inGroup = true;
			return
		}
	});
	if (!(inGroup)) {
		get_items.push(table_id)
	}
});

census_api_query_url += get_items.join(',');

var state_abbrevs = [['Alabama', 'AL'], ['Alaska', 'AK'], ['Arizona', 'AZ'], ['Arkansas', 'AR'], ['California', 'CA'], ['Colorado', 'CO'], ['Connecticut', 'CT'], ['Delaware', 'DE'], ['Florida', 'FL'], ['Georgia', 'GA'], ['Hawaii', 'HI'], ['Idaho', 'ID'], ['Illinois', 'IL'], ['Indiana', 'IN'], ['Iowa', 'IA'], ['Kansas', 'KS'], ['Kentucky', 'KY'], ['Louisiana', 'LA'], ['Maine', 'ME'], ['Maryland', 'MD'], ['Massachusetts', 'MA'], ['Michigan', 'MI'], ['Minnesota', 'MN'], ['Mississippi', 'MS'], ['Missouri', 'MO'], ['Montana', 'MT'], ['Nebraska', 'NE'], ['Nevada', 'NV'], ['New Hampshire', 'NH'], ['New Jersey', 'NJ'], ['New Mexico', 'NM'], ['New York', 'NY'], ['North Carolina', 'NC'], ['North Dakota', 'ND'], ['Ohio', 'OH'], ['Oklahoma', 'OK'], ['Oregon', 'OR'], ['Pennsylvania', 'PA'], ['Rhode Island', 'RI'], ['South Carolina', 'SC'], ['South Dakota', 'SD'], ['Tennessee', 'TN'], ['Texas', 'TX'], ['Utah', 'UT'], ['Vermont', 'VT'], ['Virginia', 'VA'], ['Washington', 'WA'], ['West Virginia', 'WV'], ['Wisconsin', 'WI'], ['Wyoming', 'WY'], ['District of Columbia', 'DC'], ['Marshall Islands', 'MH'], ['Armed Forces Africa', 'AE'], ['Armed Forces Americas', 'AA'], ['Armed Forces Canada', 'AE'], ['Armed Forces Europe', 'AE'], ['Armed Forces Middle East', 'AE'], ['Armed Forces Pacific', 'AP']];

module.exports = { demographic_data_tables_lookup,state_abbrevs, census_api_query_url };
