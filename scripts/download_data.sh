#!/usr/bin/bash

cur_dir=$(pwd)  # Checkpoint for cd back to initial dir
# Create directories if not exist 
if ! [ -d $1 ];
then
    mkdir -p $1
fi
cd $1

# data name
data_names="SalesOrderHeader SalesOrderDetail \
SpecialOffer \
SalesTerritory CountryRegion \
Product ProductSubcategory ProductCategory ProductModel \
Address StateProvince \
Employee Person EmailAddress \
Customer "
# Check data
for data in $data_names
do
    echo "Checking $data data..."
    if ! [ -f ./$data.csv ];
    then
        echo "$data database not available. Downloading..."
        wget https://raw.githubusercontent.com/Thong-Cao/etl-AdventureWorks/main/db/data/$data.csv
    else
        echo "$data database is already available."
    fi
done

cd $cur_dir
echo "Finished downloading data for pipeline."