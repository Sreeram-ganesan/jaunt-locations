LOAD h3;
LOAD spatial;
DROP TABLE IF EXISTS location_comparison;
CREATE TABLE IF NOT EXISTS location_comparison (
    duplicateid TEXT PRIMARY KEY,
    contentid1 UUID,
    contentid2 UUID,
    titles TEXT [],
    h3_index_8_1 UBIGINT,
    point1 FLOAT [],
    point2 FLOAT [],
    descriptions TEXT [],
    similarity_score FLOAT,
    similarity_score_2 FLOAT,
    distance_in_meters FLOAT
);