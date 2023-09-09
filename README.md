Este proyecto realiza la busqueda de una Api pública (https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.csv) sobre cotización diaria del Nasdaq.

Al realizar la busqueda se carga un archivo (temp.csv).
Luego se procesa ese archivo filtrando los datos posteriores al año 2.016, se 
transforma en un dataframe y se graba en el archivo 'datos_finales'.

Se realiza la conección a Redshift y se sube el dataframe a la tabla 'nasdaq_table'.

Si todo funcionó correctamente se envía un mail 
con la leyenda 'Proceso de actualización de base finalizado con éxito'.

En Api_Nasdaq.py se encuentra la definición del Dag de Airflow y las task.
En Funciones.py están las funciones de Python invocadas por las task.

En el carpeta 'Test' se encuentran imágenes de pantalla de mail enviado, mail recibido, pantalla de task de airflow y pantalla de Dbeaver con la base subida a Redshift.



