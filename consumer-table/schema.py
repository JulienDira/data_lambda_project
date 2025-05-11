from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField, IntegerType


def get_primary_key_and_schema(table_name : str):
    
    schema = {
        'AMOUNT_PER_TYPE_WINDOWED':{
            'primary_key' : 'TRANSACTION_TYPE',
            'value_schema' : StructType([
                StructField("WINDOW_START", LongType(), True),
                StructField("WINDOW_END", LongType(), True),
                StructField("TOTAL_AMOUNT", DoubleType(), True)
            ])
        },

        'COUNT_NUMB_BUY_PER_PRODUCT':{
            'primary_key' : 'PRODUCT_ID',
            'value_schema' : StructType([
                StructField("TOTAL_QUANTITY", IntegerType(), True)
            ])
        },

        'TOTAL_PAR_TRANSACTION_TYPE':{
            'primary_key' : 'TRANSACTION_TYPE',
            'value_schema' : StructType([
                StructField("TOTAL_AMOUNT", DoubleType(), True)
            ])
        },

        'TOTAL_SPENT_PER_USER_TRANSACTION_TYPE':{
            'primary_key' : 'KSQL_COL_0',
            'value_schema' : StructType([
                StructField("TOTAL_SPENT", DoubleType(), True)
            ])
        },

        'TOTAL_TRANSACTION_AMOUNT_PER_PAYMENT_METHOD':{
            'primary_key' : 'PAYMENT_METHOD',
            'value_schema' : StructType([
                StructField("TOTAL_AMOUNT", DoubleType(), True)
            ])
        },

        'TRANSACTION_STATUS_EVOLUTION':{
            'primary_key' : 'TRANSACTION_ID',
            'value_schema' : StructType([
                StructField("LATEST_STATUS", StringType(), True)
            ])
        }
    }
    return schema.get(table_name)

