cd /Users/zach/Documents/UCLA_classes/research/Polling/metro_shapefiles
# ln -s /usr/local/bin/ogr2ogr
ogr2ogr -f "GeoJSON" metro_shapes_2019.json tl_2019_us_cbsa/tl_2019_us_cbsa.shp
