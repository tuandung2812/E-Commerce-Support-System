def clean_desc(s):

    product_desc = lower(col('product_desc'))

    # chắp vá với các category có "-"
    product_desc = regexp_replace(product_desc, 'Vớ/ Tất', 'Vớ, Tất')
    product_desc = regexp_replace(product_desc, 'Quần Dài-Quần Âu', 'Quần Dài, Quần Âu')    

    # deal with "&"
    product_desc = regexp_replace(product_desc, ' &amp;', ',')

    # remove links, icon, divider
    product_desc = regexp_replace(product_desc, '<svg.*?</svg>', '')
    product_desc = regexp_replace(product_desc, '<div>', '')   
    product_desc = regexp_replace(product_desc, 'div', '')

    # Group lables into 1 row
    product_desc = regexp_replace(product_desc, '</a>', '-')  

    # I don't even remember how this work
    product_desc = regexp_replace(product_desc, '</label>', ': ')
    product_desc = regexp_replace(product_desc, 'class=', '')
    product_desc = regexp_replace(product_desc, '"', '')
    product_desc = regexp_replace(product_desc, '< ', '<')    
    product_desc = regexp_replace(product_desc, "\/.*?\>","/>")

    # Delete redundant text and spaces
    product_desc = regexp_replace(product_desc, '<label.*?>', '')
    product_desc = regexp_replace(product_desc, '<flex.*?>', '')
    product_desc = regexp_replace(product_desc, ' href=/', '')
    product_desc = regexp_replace(product_desc, '<a ', '<')
    product_desc = regexp_replace(product_desc, '<p ', '<')
    product_desc = regexp_replace(product_desc, ' +', ' ')

    #Split
    product_desc = regexp_replace(product_desc, '</>', '/')
    product_desc = regexp_replace(product_desc, '<.*?>', '')
    # s = s.split("/")
    # s = [x for x in s if x!= '']

    return s

def clean_desc(df):
    product_desc = lower(col('product_desc'))
    product_desc = regexp_replace(product_desc, 'Vớ/ Tất', 'Vớ, Tất')
    product_desc = regexp_replace(product_desc, 'Vớ/Tất', 'Vớ, Tất')
    product_desc = regexp_replace(product_desc, 'Quần Dài/Quần Âu', 'Quần Dài, Quần Âu')
    product_desc = regexp_replace(product_desc, 'Quần Dài/ Quần Âu', 'Quần Dài, Quần Âu')
    product_desc = regexp_replace(product_desc, ' &amp;', ',')
    product_desc = regexp_replace(product_desc, '<svg.*?</svg>|<div>|div|class=|"|<label.*?>|<flex.*?>| href=/', '')
    product_desc = regexp_replace(product_desc, '</a>', '-')
    product_desc = regexp_replace(product_desc, '</label>', ': ')
    product_desc = regexp_replace(product_desc, '< ', '<')
    product_desc = regexp_replace(product_desc, "\/.*?\>","/>")
    product_desc = regexp_replace(product_desc, '<a ', '<')
    product_desc = regexp_replace(product_desc, '<p ', '<')
    product_desc = regexp_replace(product_desc, ' +', ' ')

    # Split
    product_desc = regexp_replace(product_desc, '</>', '/')
    product_desc = regexp_replace(product_desc, '<.*?>', '')

    # product_desc = product_desc.split("/")
    # product_desc = [x for x in product_desc if x!= '']
    return df.withColumn('product_desc', product_desc)

df = clean_desc(df)
clean_desc(df).select("product_desc").show(100, truncate= False)