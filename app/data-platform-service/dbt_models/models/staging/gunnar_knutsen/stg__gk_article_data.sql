SELECT
    ext_article_id,
    ext_article_name_loading,
    ext_article_name_unloading,
    admmit_article_name,
    admmit_article_name_alternative
FROM {{ ref('article_data_seed') }}    
GROUP BY ext_article_id, ext_article_name_loading, ext_article_name_unloading, admmit_article_name, admmit_article_name_alternative