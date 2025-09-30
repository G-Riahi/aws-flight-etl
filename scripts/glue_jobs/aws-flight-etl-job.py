import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node renaming_columns
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
        
    # Get the first input DynamicFrame and convert to Spark DataFrame
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Collect the first row as the new column names
    new_columns = df.first()  # grabs the first row
    new_column_names = [str(c) for c in new_columns]  # convert values to strings

    # Remove the first row from the DataFrame
    df = df.tail(df.count() - 1)  # drops the first row
    df = glueContext.spark_session.createDataFrame(df)  # convert back to DataFrame

    # Rename columns
    for old, new in zip(df.columns, new_column_names):
        df = df.withColumnRenamed(old, new)
        
    # Convert back to DynamicFrame
    dyf_clean = DynamicFrame.fromDF(df, glueContext, "dyf_clean")

    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": dyf_clean}, glueContext)
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node airports-raw
airportsraw_node1759230357829 = glueContext.create_dynamic_frame.from_catalog(database="aws-flight-etl-database", table_name="airports_csv", transformation_ctx="airportsraw_node1759230357829")

# Script generated for node airlines-raw
airlinesraw_node1759230194469 = glueContext.create_dynamic_frame.from_catalog(database="aws-flight-etl-database", table_name="airlines_csv", transformation_ctx="airlinesraw_node1759230194469")

# Script generated for node flights-raw
flightsraw_node1759230301277 = glueContext.create_dynamic_frame.from_catalog(database="aws-flight-etl-database", table_name="flights_csv", transformation_ctx="flightsraw_node1759230301277")

# Script generated for node filter_airports
SqlQuery15 = '''
select
    *
from
    airports
where
    state in ('NY','NJ','PA','MA','VA','DC','MD','FL','NC','SC','GA','CT','RI','NH','VT','ME')
'''
filter_airports_node1759233370582 = sparkSqlQuery(glueContext, query = SqlQuery15, mapping = {"airports":airportsraw_node1759230357829}, transformation_ctx = "filter_airports_node1759233370582")

# Script generated for node renaming_columns
renaming_columns_node1759230914156 = MyTransform(glueContext, DynamicFrameCollection({"airlinesraw_node1759230194469": airlinesraw_node1759230194469}, glueContext))

# Script generated for node replacing_null_values_and_filtering
SqlQuery16 = '''
select 
    year,
    month,
    day,
    day_of_week,
    airline,
    flight_number,
    tail_number,
    origin_airport,
    destination_airport,
    scheduled_departure,
    COALESCE(departure_time, 0) AS departure_time,
    COALESCE(departure_delay, 0) AS departure_delay,
    COALESCE(taxi_out, 0) AS taxi_out,
    COALESCE(wheels_off, 0) AS wheels_off,
    scheduled_time,
    COALESCE(elapsed_time, 0) AS elapsed_time,
    COALESCE(air_time, 0) AS air_time,
    COALESCE(distance, 0) AS distance,
    COALESCE(wheels_on, 0) AS wheels_on,
    COALESCE(taxi_in, 0) AS taxi_in,
    scheduled_arrival,
    COALESCE(arrival_time, 0) AS arrival_time,
    COALESCE(arrival_delay, 0) AS arrival_delay,
    diverted,
    cancelled,
    cancellation_reason,
    COALESCE(air_system_delay, 0) AS air_system_delay,
    COALESCE(security_delay, 0) AS security_delay,
    COALESCE(airline_delay, 0) AS airline_delay,
    COALESCE(late_aircraft_delay, 0) AS late_aircraft_delay,
    COALESCE(weather_delay, 0) AS weather_delay
from flights
where
month <=4;
'''
replacing_null_values_and_filtering_node1759231925072 = sparkSqlQuery(glueContext, query = SqlQuery16, mapping = {"flights":flightsraw_node1759230301277}, transformation_ctx = "replacing_null_values_and_filtering_node1759231925072")

# Script generated for node Select From Collection
SelectFromCollection_node1759240605635 = SelectFromCollection.apply(dfc=renaming_columns_node1759230914156, key=list(renaming_columns_node1759230914156.keys())[0], transformation_ctx="SelectFromCollection_node1759240605635")

# Script generated for node Outwards flights
Outwardsflights_node1759233618872 = Join.apply(frame1=filter_airports_node1759233370582, frame2=replacing_null_values_and_filtering_node1759231925072, keys1=["iata_code"], keys2=["origin_airport"], transformation_ctx="Outwardsflights_node1759233618872")

# Script generated for node Incoming flights
Incomingflights_node1759233699386 = Join.apply(frame1=filter_airports_node1759233370582, frame2=replacing_null_values_and_filtering_node1759231925072, keys1=["iata_code"], keys2=["destination_airport"], transformation_ctx="Incomingflights_node1759233699386")

# Script generated for node Evaluate Data Quality (outwards flights)
EvaluateDataQualityoutwardsflights_node1759240895439_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        RowCount > 0,
        DistinctValuesCount "month" = 4,
        IsComplete "airline",
        IsComplete "diverted",
        IsComplete "cancelled",
        IsComplete "air_system_delay"
    ]
"""

EvaluateDataQualityoutwardsflights_node1759240895439 = EvaluateDataQuality().process_rows(frame=Outwardsflights_node1759233618872, ruleset=EvaluateDataQualityoutwardsflights_node1759240895439_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQualityoutwardsflights_node1759240895439", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node Evaluate Data Quality (incoming flights)
EvaluateDataQualityincomingflights_node1759234215940_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        RowCount > 0,
        DistinctValuesCount "month" = 4,
        IsComplete "airline",
        IsComplete "diverted",
        IsComplete "cancelled",
        IsComplete "air_system_delay"
    ]
"""

EvaluateDataQualityincomingflights_node1759234215940 = EvaluateDataQuality().process_rows(frame=Incomingflights_node1759233699386, ruleset=EvaluateDataQualityincomingflights_node1759234215940_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQualityincomingflights_node1759234215940", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node originalData
originalData_node1759241025977 = SelectFromCollection.apply(dfc=EvaluateDataQualityoutwardsflights_node1759240895439, key="originalData", transformation_ctx="originalData_node1759241025977")

# Script generated for node originalData
originalData_node1759241042221 = SelectFromCollection.apply(dfc=EvaluateDataQualityincomingflights_node1759234215940, key="originalData", transformation_ctx="originalData_node1759241042221")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1759240605635, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759232471650", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1759240277804 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1759240605635, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aws-flight-etl-project/proessed/airlines/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1759240277804")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=originalData_node1759241025977, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759232471650", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1759241284283 = glueContext.write_dynamic_frame.from_options(frame=originalData_node1759241025977, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aws-flight-etl-project/proessed/outwards_flights/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1759241284283")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=originalData_node1759241042221, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1759232471650", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1759241284943 = glueContext.write_dynamic_frame.from_options(frame=originalData_node1759241042221, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aws-flight-etl-project/proessed/incomming_flights/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1759241284943")

job.commit()