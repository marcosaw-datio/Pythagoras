# Pythagoras (readme beta)
App para el proceso de ingesta

Se genera un jar con maven y se ejecuta un spark submit con la siguiente línea:

- ./bin/spark-submit --packages com.databricks:spark-csv_2.10:1.5.0,com.databricks:spark-avro_2.10:2.0.1 --class com.datio.ingesta.Loader1872 /home/marcos/Documents/datio-proyects/spark/Pythagoras/spark-loader/target/spark-loader-1.0-SNAPSHOT.jar /ruta/al/json/configuracion.json
