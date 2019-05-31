!/bin/bash
for((i=1; i<=1; i++))
do
      file=/home/light/mysql/gps/gps_${i}.txt
      echo start load..... ${file} ......
mysql -h 127.0.0.1 -uroot -pxxx -e "use tt" -e "
      LOAD DATA LOCAL INFILE '${file}'
      INTO TABLE loadTable1 
      FIELDS TERMINATED BY ',' 
      LINES TERMINATED BY '\n' 
      (carflag, touchevent,opstatus,gpstime,gpslongitude,gpslatitude,gpsspeed,gpsorientation,gpsstatus);"
      sleep 3s 
done
