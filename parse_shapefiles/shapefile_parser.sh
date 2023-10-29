# ln -s /usr/local/bin/ogr2ogr
# cd /Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio/ohio_shapefiles/from_census
# ogr2ogr -f "GeoJSON" tl_2016_39_cousub/tl_2016_39_cousub.json tl_2016_39_cousub/tl_2016_39_cousub.shp
# ogr2ogr -f "GeoJSON" cb_2018_us_county_500k/tl_2013_39_place.json tl_2013_39_place/tl_2013_39_place.shp
# ogr2ogr -f "GeoJSON" cb_2018_us_county_500k/cb_2018_us_county_500k.json cb_2018_us_county_500k/cb_2018_us_county_500k.shp

# ogr2ogr -f "GeoJSON" tl_2012_39_vtd10/tl_2012_39_vtd10.json tl_2012_39_vtd10/tl_2012_39_vtd10.shp


cd /Users/zach/Downloads/cb_2018_us_county_500k
ogr2ogr -f "GeoJSON" cb_2018_us_county_500k.json cb_2018_us_county_500k.shp
