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

    cur.close()
    conn.close()

    if schema == 'raw':
        # remove the scraped_at column
        cols = [c for c in cols if c != 'scraped_at']

        if 'ncua_' in table:
            # remove id column
            cols = [c for c in cols if c != 'id']

        if table == 'fdic_institutions':
            # remove institution_id column
            cols = [c for c in cols if c != 'institution_id']

        if table == 'fdic_financials':
            # remove financials_id column
            cols = [c for c in cols if c != 'financials_id']

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
            if schema == 'raw' and 'fdic_' in table:
                d = ch['data'] # each element in the fdic data array has a 'data' key that contains the actual data
            else:
                d = ch

            # set all keys to lowercase
            d = {k.lower(): v for k, v in d.items()}

            # check that data has all the columns, if not add them with None value
            for col in cols:
                if col not in d:
                    d[col] = None

            # remove keys that are not in cols
            d = {k: v for k, v in d.items() if k in cols}

            val = ''
            for col in cols:
                if isinstance(d[col], str):
                    val += f"$${d[col]}$$, "
                else:
                    val += f"{d[col]}, "
            val = val[:-2] # remove trailing comma and space
            values += f"({val}), "

        # remove trailing comma and space
        values = values[:-2]
        
        # if table is fdic_institutions or fdic_financials, rename id column
        renamed_cols = None
        if 'fdic_' in table:
            if table == 'fdic_institutions':
                renamed_id = 'institution_id'
                
            if table == 'fdic_financials':
                renamed_id = 'financials_id'

            # rename
            renamed_cols = [c if c != 'id' else renamed_id for c in cols] 

        # replace all None values with NULL
        values = values.replace('None', 'NULL')

        sql = f"""
            INSERT INTO {schema}.{table} (
                {', '.join(renamed_cols if renamed_cols else cols)}
            ) VALUES 
                {values}
        """

        hook.run(sql=sql)
