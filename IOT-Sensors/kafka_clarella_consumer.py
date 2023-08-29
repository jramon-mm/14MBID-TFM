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
	StructField('data_owner_quality_flag', StringType(), True)
])

# Defining output base path
output_base_path = '/home/jramon/Documents/ismn/output'

# Defining checkpoint base path
checkpoint_base_path = '/home/jramon/Documents/ismn/checkpoint'

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

	df = df.select(['measurement_datetime', 'station', 'sensor_model', 'depth_from', 'measurement', 'ismn_quality_flag'])

	return df

# Main method
def main():
	# Creating spak session
	spark = SparkSession.builder.appName('KafkaSoilMoistureConsumer').getOrCreate()

	# Read messages from kafka
	kafka_df = read_from_kafka(spark)

	# Filter messages where ismn_quality_flag is not 'G'
	g_flags_df = kafka_df.filter(col('ismn_quality_flag') == 'G')

	# Select final columns
	g_flags_df = g_flags_df.select(['measurement_datetime', 'station', 'sensor_model', 'depth_from', 'measurement'])

	#output_final_path = output_base_path + '/' + topic + '/' + g_flags_df['station']
	#checkpoint_final_path = checkpoint_base_path + '/' + topic + '/' + g_flags_df['station']

	def save_batch_to_custom_path(batch_df, batch_id):
		for row in batch_df.collect():
			output_final_path = f"{output_base_path}/{topic}/{row['station']}"
			batch_df.coalesce(1).write.csv(output_final_path, mode='overwrite', header=True)

	# Start the streaming query and print the output to the console
	query = g_flags_df.writeStream.foreachBatch(save_batch_to_custom_path).option('checkpointLocation', checkpoint_base_path).start()
	#query = g_flags_df.writeStream.outputMode('append').format('csv').option('path', output_final_path).option('checkpointLocation', checkpoint_final_path).start()

	#query = g_flags_df.writeStream.outputMode('append').format('console').start()

	#query = g_flags_df.writeStream.outputMode('append').format('console').foreachBatch(save_to_single_csv).option('checkpointLocation', checkpoint_final_path).start()

	# Wait for the termination of the query (in this case, indefinitely)
	query.awaitTermination()

if __name__ == '__main__':
	main()


