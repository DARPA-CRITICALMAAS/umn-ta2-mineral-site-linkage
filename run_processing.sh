#!/bin/bash 

raw_data=${1?Raw data file missing}
attribute_map=${2?Attribute map file missing}
folder_name=$3
github_branch="new_data_$(date '+%Y%m%d')"

file_name=$(basename $raw_data)
file_name="${file_name%.*}"

# Deciding processed file save path
if [ $folder_name ]; then
    echo "Processed data will be saved as"
    echo "  "$folder_name"/"$file_name".json"
else
    folder_name='db_unknown'
    echo "File/Folder name is not declared."    
fi

echo "Processed data will be saved as"
echo "  "$folder_name"/"$file_name".json"
echo ""

# Checking existence of minmod repository
echo "Checking minmod data repository"
data_directory='../ta2-minmod-data'

if [ -d $data_directory ]; then
    echo "  "$data_directory "does exist."
else
    echo "Cloning minmod data repository"
    cd ..
    git clone git@github.com:DARPA-CRITICALMAAS/ta2-minmod-data.git
    
fi

# Create new GitHub Branch for pushing in new data
echo "Creating branch $github_branch in minmod data repository"
cd ../ta2-minmod-data
git checkout main
git pull
git checkout -b $github_branch

# Creating folder with folder name under minmod data repo
mkdir data/mineral_sites/umn/$folder_name
echo ""

# Create Docker container to run the program
echo "Creating Docker container"
cd ../umn-ta2-mineral-site-linkage
docker build -t ta2-linking .
container_id=$(docker run -dit ta2-linking)

run_script=$(cat <<END
echo "Running Preprocessing Step for FuseMine"
poetry run python3 process_data_to_schema.py --raw_data $raw_data --attribute_map $attribute_map --schema_output_filename $file_name
exit
END
)

# Running FuseMine on user input commodity
docker exec -it $container_id /bin/bash -c "$run_script"
echo ""

# Copying file from docker container to local
echo "Moving" $commodity"_sameas.csv file from Docker container to local"
docker cp $container_id":/umn-ta2-mineral-site-linkage/outputs/"$file_name".json" ../ta2-minmod-data/data/mineral_sites/umn/
echo ""

# Move to data directory
echo "Uploading processed data to minmod data repository"
cd ../ta2-minmod-data
git add --all
git commit -m "Adding preporcessed data"
git push --set-upstream origin $github_branch

git checkout main
git branch -D $github_branch
echo ""

# Stopping Docker container
echo "Terminating Docker container"
cd ../umn-ta2-mineral-site-linkage
docker stop $container_id
echo ""

echo "Please go to https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data to create a pull request for the same as links to be merged to main"