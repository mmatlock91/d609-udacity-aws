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

# Script generated for node Customers Curated
CustomersCurated_node1794590126855 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="CustomersCurated_node1794590126855")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1794590125905 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1794590125905")

# Script generated for node SQL Query
SqlQuery752 = '''
SELECT s.*
FROM step_trainer s 
INNER JOIN (
    SELECT DISTINCT serialnumber
    FROM customers
) c 
    ON s.serialnumber = c.serialnumber
'''
SQLQuery_node1794590201971 = sparkSqlQuery(glueContext, query = SqlQuery752, mapping = {"customers":CustomersCurated_node1794590126855, "step_trainer":StepTrainerLanding_node1794590125905}, transformation_ctx = "SQLQuery_node1794590201971")

# Script generated for node Amazon S3
AmazonS3_node1794590305588 = glueContext.getSink(path="s3://stedi-d609-mike-20261113/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1794590305588")
AmazonS3_node1794590305588.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1794590305588.setFormat("glueparquet", compression="snappy")
AmazonS3_node1794590305588.writeFrame(SQLQuery_node1794590201971)
job.commit()