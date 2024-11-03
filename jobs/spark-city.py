from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col


def main():
    # Initialize SparkSession with necessary configurations for connecting to Kafka and S3
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3, org.apache.hadoop:hadoop-common:3.4.0, com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
        
    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')
    
    
    # Define schema for different types of data from various sources
    # VehicleSchema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])
    
    # GpsSchema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])
    
    # TrafficSchema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])
    
    # WeatherSchema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])
    
    # EmergencySchema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])
    
    
    # Function to read data from a Kafka topic and apply the specified schema
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')    # Cast the value to a string
                .select(from_json(col('value'), schema).alias('data'))  # Parse JSON data according to the schema
                .select('data.*')   # Select all fields from the parsed data
                .withWatermark('timestamp', '2 minute'))    # Set watermark for late data handling
        
    
    # Function to write the streaming DataFrame to S3 in Parquet format
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder) # Specify checkpoint location for fault tolerance
                .option('path', output) # Specify output path in S3
                .outputMode('append')   # Set output mode to append
                .start())   # Start the streaming query
    
    # Read data from different Kafka topics
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', vehicleSchema).alias('GPS')
    trafficDF = read_kafka_topic('traffic_data', vehicleSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', vehicleSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', vehicleSchema).alias('emergency')
    
    
    AWS_BUCKET_NAME = configuration.get('AWS_BUCKET_NAME')
    # Write the streaming DataFrames to S3
    query1 = streamWriter(vehicleDF, f's3a://{AWS_BUCKET_NAME}/checkpoints/vehicle_data',
                f's3a://{AWS_BUCKET_NAME}/data/vehicle_data')
    query2 = streamWriter(gpsDF, f's3a://{AWS_BUCKET_NAME}/checkpoints/gps_data',
                f's3a://{AWS_BUCKET_NAME}/data/gps_data')
    query3 = streamWriter(trafficDF, f's3a://{AWS_BUCKET_NAME}/checkpoints/traffic_data',
                f's3a://{AWS_BUCKET_NAME}/data/traffic_data')
    query4 = streamWriter(weatherDF, f's3a://{AWS_BUCKET_NAME}/checkpoints/weather_data',
                f's3a://{AWS_BUCKET_NAME}/data/weather_data')
    query5 = streamWriter(emergencyDF, f's3a://{AWS_BUCKET_NAME}/checkpoints/emergency_data',
                f's3a://{AWS_BUCKET_NAME}/data/emergency_data')
    
    # Wait for the termination of the last query
    query5.awaitTermination()
    
if __name__== "__main__":
    main()