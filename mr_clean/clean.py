from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, LongType
from pyspark.sql.functions import lower, regexp_replace, regexp_extract, col, trim, when, count


special_char = '[^a-z0-9A-Z_àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]+'


def clean_attrs(df):
    attrs = lower(col('attrs'))
    attrs = regexp_replace(attrs, special_char, ' ')
    attrs = trim(attrs)
    df.withColumn('attrs', attrs)
    return df