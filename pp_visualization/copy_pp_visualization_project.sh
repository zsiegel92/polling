rm -rf ~/Desktop/pp_visualization
mkdir ~/Desktop/pp_visualization
mkdir ~/Desktop/pp_visualization/src
mkdir ~/Desktop/pp_visualization/data
mkdir ~/Desktop/pp_visualization/data/results
mkdir ~/Desktop/pp_visualization/data/results/maps_pps
mkdir ~/Desktop/pp_visualization/data/results/civic_results
mkdir ~/Desktop/pp_visualization/data/results/civic_results/general2020_points_and_polling_places
mkdir ~/Desktop/pp_visualization/data/shapefiles
mkdir ~/Desktop/pp_visualization/data/shapefiles/metro


# cd /Users/zach/Documents/UCLA_classes/research/Polling/voter_files/Ohio
cp -R /Users/zach/Documents/UCLA_classes/research/Polling/src/pp_visualization ~/Desktop/pp_visualization/src/pp_visualization
rm ~/Desktop/pp_visualization/src/pp_visualization/copy_pp_visualization_project.sh
rm -rf ~/Desktop/pp_visualization/src/pp_visualization/__pycache__


cp /Users/zach/Documents/UCLA_classes/research/Polling/data/shapefiles/metro/metro_shapes_2019.json ~/Desktop/pp_visualization/data/shapefiles/metro/metro_shapes_2019.json
cp /Users/zach/Documents/UCLA_classes/research/Polling/data/shapefiles/metro/metro_shapes_2019_only_properties.json ~/Desktop/pp_visualization/data/shapefiles/metro/metro_shapes_2019_only_properties.json


cd ~/Desktop
zip -r pp_visualization.zip pp_visualization
# mv ~/Desktop/pp_visualization ~/.Trash/pp_visualization
rm -rf ~/Desktop/pp_visualization
