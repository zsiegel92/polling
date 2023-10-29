# mv ~/Desktop/civic_full_project ~/.Trash/civic_full_project
rm -rf ~/Desktop/civic_full_project
mkdir ~/Desktop/civic_full_project
mkdir ~/Desktop/civic_full_project/src
mkdir ~/Desktop/civic_full_project/data
mkdir ~/Desktop/civic_full_project/data/results
mkdir ~/Desktop/civic_full_project/data/shapefiles
mkdir ~/Desktop/civic_full_project/data/shapefiles/metro
mkdir ~/Desktop/civic_full_project/data/shapefiles/cb_2018_us_county_500k
mkdir ~/Desktop/civic_full_project/data/metro_delineation
mkdir ~/Desktop/civic_full_project/data/results/civic_results
mkdir ~/Desktop/civic_full_project/data/results/civic_results/postprocessed
mkdir ~/Desktop/civic_full_project/data/results/civic_results/to_be_postprocessed
mkdir ~/Desktop/civic_full_project/data/results/civic_results/figures
mkdir ~/Desktop/civic_full_project/data/results/civic_results/Georgia
mkdir ~/Desktop/civic_full_project/data/results/civic_results/Georgia/figures



# cd /Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio
cp -R /Users/zach/Documents/UCLA_classes/research/Polling/src/civic_project ~/Desktop/civic_full_project/src/civic_project
rm ~/Desktop/civic_full_project/src/civic_project/copy_full_civic_project.sh
rm ~/Desktop/civic_full_project/src/civic_project/open_full_civic_project.sh
rm -rf ~/Desktop/civic_full_project/src/civic_project/__pycache__


cp /Users/zach/Documents/UCLA_classes/research/Polling/data/shapefiles/metro/metro_shapes_2019.json ~/Desktop/civic_full_project/data/shapefiles/metro/metro_shapes_2019.json
cp /Users/zach/Documents/UCLA_classes/research/Polling/data/shapefiles/metro/metro_shapes_2019_only_properties.json ~/Desktop/civic_full_project/data/shapefiles/metro/metro_shapes_2019_only_properties.json


cp /Users/zach/Documents/UCLA_classes/research/Polling/data/shapefiles/cb_2018_us_county_500k/cb_2018_us_county_500k.json ~/Desktop/civic_full_project/data/shapefiles/cb_2018_us_county_500k/cb_2018_us_county_500k.json
cp /Users/zach/Documents/UCLA_classes/research/Polling/data/shapefiles/cb_2018_us_county_500k/cb_2018_us_county_500k_only_properties.json ~/Desktop/civic_full_project/data/shapefiles/cb_2018_us_county_500k/cb_2018_us_county_500k_only_properties.json

cp /Users/zach/Documents/UCLA_classes/research/Polling/data/metro_delineation/list1_2020.xls ~/Desktop/civic_full_project/data/metro_delineation

cd ~/Desktop
zip -r civic_full_project.zip civic_full_project
# mv ~/Desktop/civic_full_project ~/.Trash/civic_full_project
rm -rf ~/Desktop/civic_full_project
