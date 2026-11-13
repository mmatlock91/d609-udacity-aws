import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1794587564798 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1794587564798")

# Script generated for node Customer Trusted
CustomerTrusted_node1794587597866 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1794587597866")

# Script generated for node SQL Query
SqlQuery821 = '''
SELECT a.*
FROM accelerometer a 
INNER JOIN customers c 
    ON a.`user` = c.email
'''
SQLQuery_node1794587693581 = sparkSqlQuery(glueContext, query = SqlQuery821, mapping = {"customers":CustomerTrusted_node1794587597866, "accelerometer":AccelerometerLanding_node1794587564798}, transformation_ctx = "SQLQuery_node1794587693581")

# Script generated for node Amazon S3
AmazonS3_node1794587858665 = glueContext.getSink(path="s3://stedi-d609-mike-20261113/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1794587858665")
AmazonS3_node1794587858665.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1794587858665.setFormat("glueparquet", compression="snappy")
AmazonS3_node1794587858665.writeFrame(SQLQuery_node1794587693581)
job.commit()