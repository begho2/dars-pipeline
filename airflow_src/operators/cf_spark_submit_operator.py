from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.decorators import apply_defaults

class CfSparkSubmitOperator(SparkSubmitOperator):

    template_fields = ('_application', '_conf', '_files', '_py_files', '_jars', '_driver_class_path',
                       '_packages', '_exclude_packages', '_keytab', '_principal', '_proxy_user', '_name',
                       '_application_args', '_env_vars', '_driver_memory')

    @apply_defaults
    def __init__(self, filename, sample: str = "0", *args, **kwargs):
        runtime_limit = '{{dag_run.conf["limit"] or sample}}'
        driver_memory = '{{"3g" if dag_run.conf["limit"] else "1g"}}'
        # need to add this if limit is 0 --conf "-- driver-memory 3g --conf spark.memory.fraction=0.1 "
        print(f"\nFound limit {runtime_limit}\n")
        print(f"\nFound driver_memory {driver_memory}\n")
        super(CfSparkSubmitOperator, self).__init__(
            task_id=f"SparkIngest_{filename[0:-4]}",
            # driver_class_path='some hadoop client stuff. depends where this is run',
            # files='do we need to ship this? if cluster we ship hive.site.xml, and log4j',
            # jars='', -- should not be required. we dont depend on anything else
            master="local[*]",
            # spark_binary='/usr/local/bin/spark-submit',
            # executor and driver config
            # keytab and principal location
            name=f'SparkIngest_${filename}',
            verbose=True,
            application="external_resources/dars-ingest-1.0-SNAPSHOT.jar",
            java_class="cf.ZipToDfConverterMain",
            application_args=[runtime_limit, f"input_data/{filename}", f"output_data"],
            driver_memory=driver_memory,
            conf={'spark.memory.fraction': '0.1'},

            # conn_id="might need to configure this",
            # conf="any extra options like log4j or memory fraction",
            *args,
            **kwargs

        )


    def execute(self, context):
        """
        Call the SparkSubmitHook to run the provided spark job
        """
        self._hook = SparkSubmitHook(
            conf=self._conf,
            conn_id=self._conn_id,
            files=self._files,
            py_files=self._py_files,
            archives=self._archives,
            driver_class_path=self._driver_class_path,
            jars=self._jars,
            java_class=self._java_class,
            packages=self._packages,
            exclude_packages=self._exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            proxy_user=self._proxy_user,
            name=self._name,
            num_executors=self._num_executors,
            status_poll_interval=self._status_poll_interval,
            application_args=self._application_args,
            env_vars=self._env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary
        )
        self._hook.submit(self._application)

