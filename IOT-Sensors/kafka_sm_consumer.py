#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import col, from_json

# Defining topic
topic = 'soil_moisture'

# Defining kafka options
kafka_options = {
    'kafka.bootstrap.servers': 'localhost:9092',
    'subscribe': topic,
    'startingOffsets': 'earliest'
}

# Defining the message schema based on the message structure
message_schema = StructType([
    StructField('cse_id', StringType(), True),
    StructField('network', StringType(), True),
    StructField('station', StringType(), True),
    StructField('latitude', FloatType(), True),
    StructField('longitude', FloatType(), True),
    StructField('elevation', FloatType(), True),
    StructField('depth_from', FloatType(), True),
    StructField('depth_to', FloatType(), True),
    StructField('sensor_model', StringType(), True),
    StructField('measurement_datetime', StringType(), True),
    StructField('measurement', FloatType(), True),
    StructField('ismn_quality_flag', StringType(), True),
    StructField('data_owner_quality_flag', StringType(), True),
    StructField('sensor_type', StringType(), True)
])

# Defining output base path
output_base_path = '/home/jramon/Defensa/output'

# Defining checkpoint base path
checkpoint_base_path = '/home/jramon/Defensa/checkpoint'

# Method to read messages from Kafka
def read_from_kafka(spark):
    # Read kafka messages as a dataframe
    df = spark.readStream.format('kafka').options(**kafka_options).load()

    # Convert value column from binary to string (sent as json.dumps(message).encode('utf-8'))
    df = df.withColumn('value', col('value').cast('string'))

    # Parse JSON
    df = df.withColumn('json', from_json('value', message_schema))

    # Generate df columns with JSON fields
    for field in message_schema.fields:
        df = df.withColumn(field.name, col('json')[field.name])

    df = df.select(['measurement_datetime', 'station', 'sensor_type', 'depth_from', 'measurement', 'ismn_quality_flag'])

    return df

# Defining foreachBatch function to write data to separate CSV files based on 'station'
def write_to_csv(batch_df, batch_id):
    for station_value in batch_df.select('station').distinct().rdd.flatMap(lambda x: x).collect():
        station_output_path = output_base_path + '/' + topic + '/' + station_value
        station_df = batch_df.filter(col('station') == station_value)
        station_df.coalesce(1).write.mode('append').csv(station_output_path, header=True)

# Main method
def main():
    # Creating spak session
    spark = SparkSession.builder.appName('KafkaSoilMoistureConsumer').getOrCreate()

    # Read messages from kafka
    kafka_df = read_from_kafka(spark)

    # Filter messages where ismn_quality_flag is not 'G'
    g_flags_df = kafka_df.filter(col('ismn_quality_flag') == 'G')

    # Select final columns
    g_flags_df = g_flags_df.select(['measurement_datetime', 'station', 'depth_from', 'measurement'])

    output_final_path = output_base_path + '/' + topic

    # Start the streaming query and print the output to the console
    query = g_flags_df.writeStream.outputMode('append').foreachBatch(write_to_csv).option('checkpointLocation', checkpoint_base_path).start()


    # Wait for the termination of the query (in this case, indefinitely)
    query.awaitTermination()

if __name__ == '__main__':
    main()


