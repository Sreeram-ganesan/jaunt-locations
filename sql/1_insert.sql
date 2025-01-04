INSTALL h3; LOAD h3; INSTALL spatial;LOAD spatial;
INSERT OR REPLACE INTO locations (
        contentid,
        source_id,
        title,
        subtitle,
        breadcrumb,
        category,
        subcategory,
        description,
        short_description,
        keywords,
        source_url,
        address,
        address_detailed,
        zipcode,
        phone,
        latitude,
        longitude,
        running_hours,
        featured_image_url,
        image_urls,
        additional_info,
        primary_data,
        city,
        country,
        state,
        h3_index_8,
        h3_index_12,
        is_title_duplicate
    )
SELECT contentid::UUID,
    source_id,
    title,
    subtitle,
    breadcrumb,
    category,
    subcategory,
    description,
    short_description,
    keywords,
    source_url,
    address,
    address_detailed,
    zipcode,
    phone,
    CAST(latitude AS DOUBLE),
    CAST(longitude AS DOUBLE),
    running_hours,
    featured_image_url,
    STRING_TO_ARRAY(image_urls, ',') AS image_urls,
    '{}' as additional_info,
    false AS primary_data,
    city,
    country,
    state,
    h3_latlng_to_cell(CAST(latitude AS FLOAT), CAST(longitude AS FLOAT), 8) as h3_index_8,
    h3_latlng_to_cell(CAST(latitude AS FLOAT), CAST(longitude AS FLOAT), 12) as h3_index_12,
    false AS is_title_duplicate
FROM read_json(
        '%s',
        ignore_errors = true
    )