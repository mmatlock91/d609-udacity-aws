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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1794589505623 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1794589505623")

# Script generated for node customer_trusted
customer_trusted_node1794589502048 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1794589502048")

# Script generated for node SQL Query
SqlQuery710 = '''
SELECT DISTINCT c.*
FROM customers c 
INNER JOIN accelerometer a 
    ON c.email = a.`user`
'''
SQLQuery_node1794589581006 = sparkSqlQuery(glueContext, query = SqlQuery710, mapping = {"accelerometer":accelerometer_trusted_node1794589505623, "customers":customer_trusted_node1794589502048}, transformation_ctx = "SQLQuery_node1794589581006")

# Script generated for node Amazon S3
AmazonS3_node1794589673291 = glueContext.getSink(path="s3://stedi-d609-mike-20261113/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1794589673291")
AmazonS3_node1794589673291.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
AmazonS3_node1794589673291.setFormat("glueparquet", compression="snappy")
AmazonS3_node1794589673291.writeFrame(SQLQuery_node1794589581006)
job.commit()