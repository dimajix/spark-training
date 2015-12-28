## Reimport Data

```sql
sqoop import \
    --connect jdbc:mysql://localhost/training \
    --username root --password cloudera \
    --table ish \
    --columns usaf,wban,name,country,fips,state,callname,latitude,longitude,elevation,date_begin,date_end \
    --fetch-size 1000 \
    --num-mappers 1\
    --as-parquetfile \
    --hive-import \
    --create-hive-table \
    --hive-database training \
    --hive-table ish_reimport
```
