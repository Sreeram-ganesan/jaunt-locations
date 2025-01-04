INSTALL spatial;
LOAD spatial;
INSERT INTO location_comparison (
        duplicateid,
        contentid1,
        contentid2,
        titles,
        h3_index_8_1,
        point1,
        point2,
        descriptions,
        similarity_score,
        distance_in_meters
    )
SELECT distinct Concat(l1.contentid,l2.contentid) as duplicateid,
    l1.contentid AS contentid1,
    l2.contentid AS contentid2,
    ARRAY [l1.title, l2.title] AS titles,
    l1.h3_index_8 AS h3_index_8_1,
    ARRAY [l1.latitude, l1.longitude] AS point1,
    ARRAY [l2.latitude, l2.longitude] AS point2,
    ARRAY [l1.description, l2.description] AS descriptions,
    jaro_winkler_similarity(l1.title, l2.title) AS similarity_score,
    ST_DISTANCE_SPHEROID(
        ST_POINT(l1.latitude, l1.longitude),
        ST_POINT(l2.latitude, l2.longitude)
    ) AS distance_in_meters
FROM locations l1
JOIN locations l2 ON l1.h3_index_8 = l2.h3_index_8 AND l1.contentid < l2.contentid
LEFT JOIN location_comparison lc ON Concat(l1.contentid,l2.contentid) = lc.duplicateid
WHERE lc.duplicateid IS NULL
ORDER BY l1.h3_index_8;