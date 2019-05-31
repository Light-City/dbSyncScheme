#!/bin/bash
source /etc/profile
up=185941000
for((i=1; i>0; i++))
do   
    start=$(((${i} - 1) * 40000 + 1))
    end=$((${i} * 40000))
    if [ $end -ge $up ]
    then 
        end=185941000
    fi

    sql="select id,carflag, touchevent, opstatus,gpstime,gpslongitude,gpslatitude,gpsspeed,gpsorientation,gpsstatus from loaddb.loadTable1 where id>=${start} and id<=${end} and \$CONDITIONS";  
    
    sqoop import --connect jdbc:mysql://localhost:3306/loaddb --username root --password xxx --query "${sql}" --hbase-row-key id --hbase-create-table --column-family info --hbase-table mysql_data --split-by id  -m 4
    echo Sqoop import from: ${start} to: ${end} success....................................
    if [ $end -eq $up ]
    then 
        break
    fi

done
