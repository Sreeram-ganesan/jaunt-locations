Drop table if exists locations;
CREATE TABLE IF NOT EXISTS locations (
    contentid UUID PRIMARY KEY,
    source_id TEXT,
    title TEXT,
    subtitle TEXT,
    breadcrumb TEXT,
    category TEXT,
    subcategory TEXT,
    description TEXT,
    short_description TEXT,
    keywords TEXT,
    source_url TEXT,
    address TEXT,
    address_detailed TEXT,
    zipcode TEXT,
    phone TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    running_hours TEXT,
    featured_image_url TEXT,
    image_urls TEXT [],
    additional_info JSON,
    primary_data BOOLEAN,
    city TEXT,
    country TEXT,
    state TEXT,
    h3_index_8 UBIGINT,
    h3_index_12 UBIGINT,
    word_count INT,
    tag TEXT default 'blue',
    is_title_duplicate BOOLEAN,
    embeddings JSON default '[]',
);