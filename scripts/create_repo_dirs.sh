#!/usr/bin/bash

repo_dirs="./logs/airflow ./logs/hiveserver ./data"

for dir_path in $repo_dirs
do
    echo "Creating and setting permissions for directory $dir_path"
    mkdir -p $dir_path
    chmod +rw -R $dir_path
done

echo "Finished creating directories for repository"