from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.decorators import apply_defaults

class CfSparkSubmitOperator(SparkSubmitOperator):

    @apply_defaults
    def __init__(self, filename, sample: str = "0", *args, **kwargs):
        runtime_limit = '{{dag_run.conf["limit"] or sample}}'
        # driver_memory = '{{"3g" if sample==0 else "1g"}}'
        # need to add this if limit is 0 --conf "-- driver-memory 3g --conf spark.memory.fraction=0.1 "
        print(f"\nFound limit {runtime_limit}\n")
        super(CfSparkSubmitOperator, self).__init__(
            task_id=f"SparkIngest_{filename[0:-4]}",
            # driver_class_path='some hadoop client stuff. depends where this is run',
            # files='do we need to ship this? if cluster we ship hive.site.xml, and log4j',
            # jars='', -- should not be required. we dont depend on anything else
            master="local[4]",
            # spark_binary='/usr/local/bin/spark-submit',
            # executor and driver config
            # keytab and principal location
            name=f'SparkIngest_${filename}',
            verbose=True,
            application="external_resources/dars-ingest-1.0-SNAPSHOT.jar",
            java_class="cf.ZipToDfConverterMain",
            application_args=[runtime_limit, f"input_data/{filename}", f"output_data"],
            # driver_memory=driver_memory,
            # conf={'spark.memory.fraction': '0.1'},

            # conn_id="might need to configure this",
            # conf="any extra options like log4j or memory fraction",
            *args,
            **kwargs

        )

