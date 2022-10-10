Mix.install([:jason, :nimble_options])

defmodule Migrator do
  require Logger

  @opts_schema [
    source: :string,
    destination: :string,
    iam_role: :string,
    db_user: :string,
    db_name: :string,
    s3_bucket: :string
  ]

  def run do
    options = parse_options()

    create_missing_schemas(options)
    create_missing_tables(options)
    migrate_data(options)
  end

  defp parse_options do
    {options, _rest} =
      OptionParser.parse!(System.argv(),
        strict: @opts_schema,
        aliases: [
          s: :source,
          d: :destination,
          r: :iam_role,
          u: :db_user,
          n: :db_name,
          b: :s3_bucket
        ]
      )

    options
    |> NimbleOptions.validate!(
      Enum.map(@opts_schema, fn {name, type} -> {name, [type: type, required: true]} end)
    )
    |> Map.new()
  end

  defp migrate_data(options) do
    list_tables(options.source, options)
    |> Enum.each(fn {schema, table} ->
      Logger.info("Starting migration for [#{schema}, #{table}]")
      unload_to_s3(schema, table, options)
      copy_from_s3(schema, table, options)
      clean_duplicates(schema, table, options)
    end)
  end

  defp copy_from_s3(schema, table, options) do
    options.destination
    |> run_statement(
      "COPY #{schema}.#{table} FROM 's3://#{options.s3_bucket}/#{schema}/#{table}/' IAM_ROLE '#{options.iam_role}' format as json 'auto';",
      options
    )
  end

  defp clean_duplicates(schema, table, options) do
    options.destination
    |> run_statement(
      "DELETE FROM #{schema}.#{table} USING #{schema}.#{table} b WHERE #{schema}.#{table}.uuid > b.uuid AND #{schema}.#{table}.id = b.id;",
      options
    )
  end

  defp unload_to_s3(schema, table, options) do
    options.source
    |> run_statement(
      "UNLOAD ('SELECT * FROM #{schema}.#{table}') TO 's3://#{options.s3_bucket}/#{schema}/#{table}/' IAM_ROLE '#{options.iam_role}' JSON CLEANPATH;",
      options
    )
  end

  defp create_missing_tables(options) do
    old_tables = list_tables(options.source, options)
    new_tables = list_tables(options.destination, options)

    missing_tables = difference(old_tables, new_tables)

    Enum.each(missing_tables, fn {schema, table} ->
      ddl = get_ddl(schema, table, options)

      run_statement(options.destination, ddl, options)
    end)
  end

  defp get_ddl(schema, table, options) do
    options.source
    |> run_statement(
      "SELECT ddl FROM admin.v_generate_tbl_ddl WHERE schemaname = '#{schema}' AND tablename = '#{table}' ORDER BY seq",
      options
    )
    |> Map.fetch!("Records")
    |> Enum.map(fn [%{"stringValue" => ddl}] -> ddl end)
    |> Enum.join("\n")
  end

  defp list_tables(cluster, options) do
    cluster
    |> run_statement(
      "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'admin');",
      options
    )
    |> Map.fetch!("Records")
    |> Enum.map(fn [%{"stringValue" => schema}, %{"stringValue" => table}] ->
      {schema, table}
    end)
  end

  defp create_missing_schemas(options) do
    Logger.info("Creating missing schemas")

    old_schemas =
      options.source
      |> list_schemas(options)

    new_schemas =
      options.destination
      |> list_schemas(options)

    missing_schemas = difference(old_schemas, new_schemas)

    Enum.each(missing_schemas, fn schema ->
      run_statement(options.destination, "CREATE SCHEMA #{schema}", options)
    end)
  end

  defp difference(a, b) do
    MapSet.difference(MapSet.new(a), MapSet.new(b)) |> Enum.to_list()
  end

  defp list_schemas(cluster, options) do
    cluster
    |> run_statement("SELECT schema_name FROM information_schema.schemata;", options)
    |> Map.fetch!("Records")
    |> Enum.map(fn record -> Enum.at(record, 0) |> Map.fetch!("stringValue") end)
  end

  defp run_statement(cluster, statement, options) do
    %{"Id" => id} = execute_statement(cluster, statement, options)
    check_statement(id)
  end

  defp check_statement(id) do
    case describe_statement(id) do
      %{"Status" => "STARTED"} ->
        Process.sleep(100)
        check_statement(id)

      %{"Status" => "FINISHED", "HasResultSet" => false} = status ->
        status

      %{"Status" => "FINISHED"} ->
        get_statement_result(id)
    end
  end

  defp execute_statement(cluster, statement, options) do
    {result, 0} =
      cmd("aws", [
        "redshift-data",
        "execute-statement",
        "--cluster-identifier",
        cluster,
        "--database",
        options.db_name,
        "--db-user",
        options.db_user,
        "--sql",
        statement
      ])

    Jason.decode!(result)
  end

  defp describe_statement(statement_id) do
    {result, 0} = cmd("aws", ["redshift-data", "describe-statement", "--id", statement_id])

    Jason.decode!(result)
  end

  defp get_statement_result(statement_id) do
    {result, 0} = cmd("aws", ["redshift-data", "get-statement-result", "--id", statement_id])

    Jason.decode!(result)
  end

  defp cmd(command, args) do
    Logger.info("Command: #{command} #{Enum.join(args, "\s")}")

    {result, exit_code} = System.cmd(command, args)

    Logger.debug("Exit code: #{exit_code}")
    Logger.debug("Result: #{result}")

    {result, exit_code}
  end
end

Migrator.run()