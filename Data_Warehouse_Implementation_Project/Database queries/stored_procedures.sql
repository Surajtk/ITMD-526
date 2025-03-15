USE DATABASE CARS_DATASET;   -- Set your database
USE SCHEMA public;          -- Set your schema (replace 'public' with your schema name)

CREATE OR REPLACE PROCEDURE Update_SCD_Type2 (
    p_table_name VARCHAR,
    p_id_column VARCHAR,
    p_old_id NUMBER,
    p_new_values VARIANT -- Use VARIANT to handle JSON-like data
)
RETURNS STRING
LANGUAGE SQL
AS 
$$
DECLARE v_sql STRING;
BEGIN
    -- Step 1: Expire the existing record
    v_sql := 'UPDATE ' || p_table_name || ' 
              SET End_Date = CURRENT_DATE, Is_Current = ''N''
              WHERE ' || p_id_column || ' = ' || p_old_id || ' 
              AND Is_Current = ''Y'';';
    EXECUTE IMMEDIATE v_sql;

    -- Step 2: Insert the new version with the values from VARIANT (JSON-like data)
    v_sql := 'INSERT INTO ' || p_table_name || ' 
              SELECT ' || TO_VARIANT(p_new_values) || ' , CURRENT_DATE AS Effective_Date, NULL AS End_Date, ''Y'' AS Is_Current 
              FROM ' || p_table_name || '
              WHERE ' || p_id_column || ' = ' || p_old_id || ';';
    EXECUTE IMMEDIATE v_sql;

    RETURN 'SCD Type 2 Update Completed for ' || p_table_name;
END;
$$;

