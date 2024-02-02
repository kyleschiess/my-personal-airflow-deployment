import json

def insert_to_pg(hook, schema, table, data):
    # get list of columns from table
    col_sql = f"""
        SELECT 
            column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
    """
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(col_sql)
    cols = [c[0] for c in cur.fetchall()]

    # remove id and scraped_at columns
    cols.remove('id')
    cols.remove('scraped_at')

    cur.close()
    conn.close()

    bulk_insert_to_sql(
        hook=hook,
        schema=schema,
        table=table,
        cols=cols,
        data=data,
    )

def bulk_insert_to_sql(hook, schema, table, cols, data):
    # chunks of 10000
    for i in range(0, len(data), 10000):
        chunk = data[i:i+10000]

        values = ''
        for ch in chunk:
            val = ''
            for col in cols:
                if isinstance(ch[col], str):
                    val += f"$${ch[col]}$$, "
                elif isinstance(ch[col], object):
                    j = json.dumps(ch[col])
                    val += f"$${j}$$, "
                else:
                    val += f"{ch[col]}, "
            val = val[:-2] # remove trailing comma and space
            values += f"({val}), "

        # remove trailing comma and space
        values = values[:-2]

        # replace all 'None' values with NULL
        values = values.replace("'None'", 'NULL')

        # replace all 'null' values with NULL
        values = values.replace("'null'", 'NULL')

        sql = f"""
            INSERT INTO {schema}.{table} (
                {', '.join(cols)}
            ) VALUES 
                {values}
        """

        hook.run(sql=sql)
