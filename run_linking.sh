#!/bin/bash 

source ~/.bashrc

commodity=${1?Commodity parameter missing}
github_branch="commodity_sameas_$(date '+%Y%m%d')"

echo "Checking minmod data repository"
data_directory='../ta2-minmod-data'

if [ -d $data_directory ]; then
    echo "  "$data_directory "does exist."
    cd ..
else
    echo "Cloning minmod data repository"
    cd ..
    git clone git@github.com:DARPA-CRITICALMAAS/ta2-minmod-data.git
    echo "  Completed"
fi

sleep 1s

# Create new GitHub Branch for pushing in new data
echo "Creating branch $github_branch in minmod data repository"
cd ta2-minmod-data
git checkout main
git pull
git checkout -b $github_branch
echo ""

# Create Docker container to run the program
echo "Creating Docker container"
cd ../umn-ta2-mineral-site-linkage
docker build -t ta2-linking .
container_id=$(docker run -dit ta2-linking)

run_script=$(cat <<END
echo "Running FuseMine for commodity: "$commodity
poetry run python3 fusemine.py --commodity $commodity
exit
END
)

# Running FuseMine on user input commodity
docker exec -it $container_id /bin/bash -c "$run_script"
echo "  Completed"
echo ""

# Copying file from docker container to local
echo "Moving" $commodity"_sameas.csv file from Docker container to local"
docker cp $container_id":/umn-ta2-mineral-site-linkage/outputs/"$commodity"_sameas.csv" ../ta2-minmod-data/data/same-as/umn/
echo "  Completed"
echo ""

# Move to data directory
echo "Uploading sameas link to minmod data repository"
cd ../ta2-minmod-data
git add --all
git commit -m "Adding sameas link"
git push --set-upstream origin $github_branch

git checkout main
git branch -D $github_branch
echo "  Completed"
echo ""

# Stopping Docker container
echo "Terminating Docker container"
cd ../umn-ta2-mineral-site-linkage
docker stop $container_id
echo "  Completed"
echo ""

echo "Please go to https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data to create a pull request for the same as links to be merged to main"
