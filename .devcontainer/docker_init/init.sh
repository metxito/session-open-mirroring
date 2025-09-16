#!/bin/bash
set -e

echo -e "\033[32mWaiting 30 seconds\033[0m"
sleep 30

echo -e "\033[32mPost script has started\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"



echo ""
echo ""
echo -e "\033[32msudo apt-get update\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo apt-get update



echo ""
echo ""
echo -e "\033[32msudo apt-get install -y unixodbc\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo apt-get install -y unixodbc



echo ""
echo ""
echo -e "\033[32msudo apt-get install -y unixodbc-dev\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo apt-get install -y unixodbc-dev



echo ""
echo ""
echo -e "\033[32msudo apt-get install -y curl\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo apt-get install -y curl



echo ""
echo ""
echo -e "\033[32msudo apt-get install -y apt-transport-https\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo apt-get install -y apt-transport-https



echo ""
echo ""
echo -e "\033[32msudo apt-get install -y gnupg\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo apt-get install -y gnupg



## Add Microsoft repo key
echo ""
echo ""
echo -e "\033[32mcurl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -



### Use Microsoft repo for Debian 12 (bookworm)
### maybe it is necessary to update this for the proper machine
#curl https://packages.microsoft.com/config/debian/12/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
echo ""
echo ""
echo -e "\033[32mcurl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list



## Update again and install SQL Server ODBC Driver 18
echo ""
echo ""
echo -e "\033[32msudo apt-get update\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo apt-get update



## Install Python dependencies
echo ""
echo ""
echo -e "\033[32mpip install --upgrade pip\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
pip install --upgrade pip



## Install msodbcsql18
echo ""
echo ""
echo -e "\033[32msudo ACCEPT_EULA=Y apt-get install -y msodbcsql18\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18



## Install mssql-tools18
echo ""
echo ""
echo -e "\033[32msudo ACCEPT_EULA=Y apt-get install -y mssql-tools18\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18


## Install git
echo ""
echo ""
echo -e "\033[32msudo ACCEPT_EULA=Y apt-get install -y git\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
sudo ACCEPT_EULA=Y apt-get install -y git


## Install python requirements
echo ""
echo ""
echo -e "\033[32mpip install -r /docker_init/requirements.txt\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
pip install -r /docker_init/requirements.txt



## RUN SQL scripts
echo ""
echo ""
echo -e "\033[32mDatabase [emfcc_source_basic] initializing\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
/opt/mssql-tools18/bin/sqlcmd \
    -S emfcc-sqlserver \
    -U sa \
    -P 'EMFcc2025!' \
    -i /docker_init/1000_emfcc_source_basic_init_database.sql \
    -C || echo "Init script failed"




echo ""
echo ""
echo -e "\033[32mDatabase [emfcc_source_cdc] initializing\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
/opt/mssql-tools18/bin/sqlcmd \
    -S emfcc-sqlserver \
    -U sa \
    -P 'EMFcc2025!' \
    -i /docker_init/1000_emfcc_source_cdc_init_database.sql \
    -C || echo "Init script failed"



echo ""
echo ""
echo -e "\033[32mDatabase [emfcc_control] initializing\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
/opt/mssql-tools18/bin/sqlcmd \
    -S emfcc-sqlserver \
    -U sa \
    -P 'EMFcc2025!' \
    -i /docker_init/1000_emfcc_control_init_database.sql \
    -C || echo "Init script failed"


echo ""
echo -e "\033[32mDatabase finished\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"
echo ""
echo ""
echo -e "\033[34m-------------------------------------\033[0m"
echo -e "\033[32m      All the preparation is done\033[0m"
echo -e "\033[34m-------------------------------------\033[0m"




