{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "ce3f40be",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Start spark\n",
    "import findspark\n",
    "findspark.init()\n",
    "import pyspark\n",
    "sc = pyspark.SparkContext()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "fc2c6dfb",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Start spark Import libraries\n",
    "from pyspark import SparkConf, SparkContext,SparkFiles\n",
    "from pyspark.sql.functions import col, isnan,to_date,explode,concat_ws,expr,split,trim, length\n",
    "from pyspark.sql import SparkSession, functions as F\n",
    "from urllib.request import urlopen\n",
    "import os"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "b82a2416",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Configuration of spark session, take in account that this is running on a Windows machine.\n",
    "#In mac os x and linus most likely this configuration must be changed\n",
    "os.environ[\"HADOOP_HOME\"] = \"file:///C:/hadoop/bin/\"\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"YourAppName\") \\\n",
    "    .config(\"spark.driver.memory\", \"6g\") \\\n",
    "    .config(\"spark.executor.memory\", \"6g\") \\\n",
    "    .getOrCreate()\n",
    "#Data is in this github repository\n",
    "vehicle_json=\"https://raw.githubusercontent.com/ctasaycos/Python/main/Data/electric_vehicle_population_data.json\"\n",
    "jsonData = urlopen(vehicle_json).read().decode('utf-8')\n",
    "rdd = spark.sparkContext.parallelize([jsonData])\n",
    "df = spark.read.json(rdd)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "30985ef0",
   "metadata": {},
   "outputs": [],
   "source": [
    "#The nested json file has metadata and data separately\n",
    "df_transformed = df.select(\n",
    "    col(\"data\"),\n",
    "    col(\"meta.view.approvals\").alias(\"approvals\"),\n",
    "    col(\"meta.view.assetType\").alias(\"assetType\"),\n",
    "    col(\"meta.view.attribution\").alias(\"attribution\"),\n",
    "    col(\"meta.view.averageRating\").alias(\"\"),\n",
    "    col(\"meta.view.category\").alias(\"category\"),\n",
    "    col(\"meta.view.clientContext\").alias(\"clientContext\"),\n",
    "    col(\"meta.view.columns\").alias(\"columns\"),\n",
    "    col(\"meta.view.createdAt\").alias(\"createdAt\"),\n",
    "    col(\"meta.view.description\").alias(\"description\"),\n",
    "    col(\"meta.view.displayType\").alias(\"displayType\"),\n",
    "    col(\"meta.view.downloadCount\").alias(\"downloadCount\"),\n",
    "    col(\"meta.view.flags\").alias(\"viewFlags\"),\n",
    "    col(\"meta.view.grants\").alias(\"grants\"),\n",
    "    col(\"meta.view.hideFromCatalog\").alias(\"hideFromCatalog\"),\n",
    "    col(\"meta.view.hideFromDataJson\").alias(\"hideFromDataJson\"),\n",
    "    col(\"meta.view.id\").alias(\"viewId\"),\n",
    "    col(\"meta.view.license\").alias(\"license\"),\n",
    "    col(\"meta.view.licenseId\").alias(\"licenseId\"),\n",
    "    col(\"meta.view.metadata\").alias(\"metadata\"),\n",
    "    col(\"meta.view.name\").alias(\"viewName\"),\n",
    "    col(\"meta.view.newBackend\").alias(\"newBackend\"),\n",
    "    col(\"meta.view.numberOfComments\").alias(\"numberOfComments\"),\n",
    "    col(\"meta.view.oid\").alias(\"oid\"),\n",
    "    col(\"meta.view.owner\").alias(\"owner\"),\n",
    "    col(\"meta.view.provenance\").alias(\"provenance\"),\n",
    "    col(\"meta.view.publicationAppendEnabled\").alias(\"publicationAppendEnabled\"),\n",
    "    col(\"meta.view.publicationDate\").alias(\"publicationDate\"),\n",
    "    col(\"meta.view.publicationGroup\").alias(\"publicationGroup\"),\n",
    "    col(\"meta.view.publicationStage\").alias(\"publicationStage\"),\n",
    "    col(\"meta.view.rights\").alias(\"rights\"),\n",
    "    col(\"meta.view.rowsUpdatedAt\").alias(\"rowsUpdatedAt\"),\n",
    "    col(\"meta.view.rowsUpdatedBy\").alias(\"rowsUpdatedBy\"),\n",
    "    col(\"meta.view.tableAuthor\").alias(\"tableAuthor\"),\n",
    "    col(\"meta.view.tableId\").alias(\"tableId\"),\n",
    "    col(\"meta.view.tags\").alias(\"tags\"),\n",
    "    col(\"meta.view.totalTimesRated\").alias(\"totalTimesRated\"),\n",
    "    col(\"meta.view.viewCount\").alias(\"viewCount\"),\n",
    "    col(\"meta.view.viewLastModified\").alias(\"viewLastModified\"),\n",
    "    col(\"meta.view.viewType\").alias(\"viewType\")\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "d68bbf36",
   "metadata": {},
   "outputs": [],
   "source": [
    "#In this case we only need the data and columns (this field contains the headers)\n",
    "df_transformed=df_transformed.select(\"data\",\"columns\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "1420ebb9",
   "metadata": {},
   "outputs": [],
   "source": [
    "#We get the headers and stored them in a list, also cleaned the data\n",
    "df_headers=df_transformed.select(\"columns\", explode(\"columns\").alias(\"exploded_column\")) \\\n",
    "    .select(\"exploded_column.id\", \"exploded_column.name\", \"exploded_column.description\") \\\n",
    "    .filter((col(\"description\").isNotNull()) & (length(trim(col(\"description\"))) > 0))\n",
    "list_headers = df_headers.select(\"name\").rdd.flatMap(lambda x: x).collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "ad894548",
   "metadata": {},
   "outputs": [],
   "source": [
    "#We only need relevant data we will not pull all.\n",
    "df_data = (\n",
    "    df_transformed\n",
    "    .select(explode(\"data\").alias(\"flattened_data\"))\n",
    "    .withColumn(\n",
    "        \"processed_data\",\n",
    "        expr(\"CONCAT_WS(', ', transform(flattened_data, x -> IFNULL(x, '')))\")\n",
    "    )\n",
    "    .select(\n",
    "        split(col(\"processed_data\"), \", \")[8].alias(\"col9\"),\n",
    "        split(col(\"processed_data\"), \", \")[9].alias(\"col10\"),\n",
    "        split(col(\"processed_data\"), \", \")[10].alias(\"col11\"),\n",
    "        split(col(\"processed_data\"), \", \")[11].alias(\"col12\"),\n",
    "        split(col(\"processed_data\"), \", \")[12].alias(\"col13\"),\n",
    "        split(col(\"processed_data\"), \", \")[13].alias(\"col14\"),\n",
    "        split(col(\"processed_data\"), \", \")[14].alias(\"col15\"),\n",
    "        split(col(\"processed_data\"), \", \")[15].alias(\"col16\"),\n",
    "        split(col(\"processed_data\"), \", \")[16].alias(\"col17\"),\n",
    "        split(col(\"processed_data\"), \", \")[17].alias(\"col18\"),\n",
    "        split(col(\"processed_data\"), \", \")[18].alias(\"col19\"),\n",
    "        split(col(\"processed_data\"), \", \")[19].alias(\"col20\"),\n",
    "        split(col(\"processed_data\"), \", \")[20].alias(\"col21\"),\n",
    "        split(col(\"processed_data\"), \", \")[21].alias(\"col22\"),\n",
    "        split(col(\"processed_data\"), \", \")[22].alias(\"col23\"),\n",
    "        split(col(\"processed_data\"), \", \")[23].alias(\"col24\"),\n",
    "        split(col(\"processed_data\"), \", \")[24].alias(\"col25\")\n",
    "    )\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "ca4e3730",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Use the real headers\n",
    "df_export = df_data.toDF(*list_headers)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "cce02aa6",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "#In this project we push the data in parquet\n",
    "local_parquet_path = \"file:///C:/Users/ctasa/Pyspark/electric_vehicle_population_data/parquet\"\n",
    "df_export.write.parquet(local_parquet_path, mode=\"overwrite\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "45c8094e",
   "metadata": {},
   "outputs": [],
   "source": [
    "#To display to the stakeholders we show a sample of the data in csv\n",
    "df_sample = df_export.sample(fraction=0.01, seed=42)\n",
    "csv_path = \"file:///C:/Users/ctasa/Pyspark/electric_vehicle_population_data/csv\"\n",
    "df_sample.write.csv(csv_path, header=True, mode=\"overwrite\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
