# redshift-migrator

Migrate data between redshift clusters.

## How it works

1. Create the any schemas and tables of the source cluster in the destination cluster.
2. [Unload](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) each table from the source cluster into a s3 bucket.
3. [Copy](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) each table from the s3 bucket into the destination cluster.
4. Delete any duplicate data in each table using the `id` column as uniqueness key.

## Requirements

- elixir 1.13+ 
- aws cli
- Source and Destination Redshift Clusters with the same user name and database name.
- Add a role to both clusters to read/write to the s3 bucket that the data will be unloaded
- Create the [admin View to get the DDL for a table](https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql#L54) on the source cluster.

## Usage

``` sh
$ elixir migrator.exs -s source-redshift-cluster -d destination-redshift-cluster -r "arn:aws:iam::$ACCOUNT_ID:role/service-role/$ROLE_NAME" -u $DB_USER -n $DB_NAME -b $S3_BUCKET 
```

