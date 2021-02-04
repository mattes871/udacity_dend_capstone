# Ideas and open tasks

1)  Try out airflow.providers.amazon.aws.hook.s3 to
    a) get list of keys in S3 bucket
    b) load certain files from bucket using the key obtained in a)


- Check if it is possible to transfer data directly from S3 to Postgres without
  storing on an intermediate Staging Area.


- Isolate target DB parameters to enable an easier change of database engine
  lateron, e.g. change from a local Postgresql to AWS Redshift
- Check which Executor is used (should be LocalExecutor not Serial)

