from pyspark.sql.functions import lower, regexp_replace, regexp_extract, col, trim, when, count, instr

special_char = '[^a-z0-9A-Z_' \
               'àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬ' \
               'ÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]+'


def k_to_number(c):  # 3,2k -> 3200  
    contain_comma = instr(c, ',') >= 1
    c = when(contain_comma, regexp_replace(c, 'k', '00'))\
        .otherwise(regexp_replace(c, 'k', '000'))
    c = regexp_replace(c, ',', '')
    return c


def clean_attrs(df):
    attrs = lower(col('attrs'))
    attrs = regexp_replace(attrs, special_char, ' ')
    attrs = trim(attrs)
    return df.withColumn('attrs', attrs)


def extract_shop_like_tier(df):
    shop_like_tier = regexp_extract(col('shop_info'), '^(Yêu Thích\+?)\n', 1)
    shop_like_tier = when(shop_like_tier == "Yêu Thích+", 2) \
        .when(shop_like_tier == "Yêu Thích", 1) \
        .otherwise(0)
    return df.withColumn('shop_like_tier', shop_like_tier)


def extract_shop_num_review(df):
    shop_num_review = regexp_extract(col('shop_info'), 'Đánh Giá\n(.+)\n', 1)
    shop_num_review = k_to_number(shop_num_review)
    shop_num_review = shop_num_review.cast('int')
    return df.withColumn('shop_num_review', shop_num_review)


def extract_shop_reply_percectage(df):
    shop_reply_percentage = regexp_extract(col('shop_info'), 'Tỉ Lệ Phản Hồi\n(.+)\n', 1)
    shop_reply_percentage = regexp_replace(shop_reply_percentage, '%', '')
    shop_reply_percentage = shop_reply_percentage.cast('float') / 100
    return df.withColumn('shop_reply_percentage', shop_reply_percentage)


def extract_shop_reply_time(df):
    shop_reply_time = regexp_extract(col('shop_info'), 'Thời Gian Phản Hồi\n(.+)\n', 1)
    return df.withColumn(shop_reply_time)



def extract_shop_creation_time(df):
    shop_creation_time = regexp_extract(col('shop_info'), 'Tham Gia\n(.+)\n', 1)
    num = regexp_extract(shop_creation_time, '\d+', 0).cast('int')
    contain_year = instr(shop_creation_time, 'năm') >= 1
    contain_month = instr(shop_creation_time, 'tháng') >= 1
    shop_creation_time = when(contain_year, num * 12)\
        .otherwise(when(contain_month, num)\
        .otherwise(lit(0)))
    return df.withColumn("shop_creation_time", shop_creation_time)



def extract_shop_num_follower(df):
    shop_num_follower = regexp_extract(col('shop_info'), 'Người Theo Dõi\n(.+)', 1)
    shop_num_follower = k_to_number(shop_num_follower)
    shop_num_follower = shop_num_follower.cast('int')
    return df.withColumn("shop_num_follower", shop_num_follower)