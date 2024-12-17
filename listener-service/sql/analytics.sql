/* Filters saga 'got' and by initial and final timestamp */
SELECT e.entity_id, e.name, sum(r.entity_counter) as total_entity_occurrences, sum(r.readers) as total_readers
FROM fact_readers r
    INNER JOIN dim_entity e ON e.entity_id = r.entity_id
    INNER JOIN dim_saga s ON s.saga_id = r.saga_id
WHERE s.name = 'got' AND r.date_time >= '2024-03-06 23:24:34' AND r.date_time <= '2024-03-10 23:41:43'
GROUP BY e.name, e.entity_id
ORDER BY total_entity_occurrences DESC
LIMIT 8 -- TOP N entities

/* Filters saga 'lotr' and by initial and final timestamp */
SELECT e.entity_id, e.name, sum(r.entity_counter) as total_entity_occurrences, sum(r.readers) as total_readers
FROM fact_readers r
    INNER JOIN dim_entity e ON e.entity_id = r.entity_id
    INNER JOIN dim_saga s ON s.saga_id = r.saga_id
WHERE s.name = 'lotr' AND r.date_time >= '2024-03-06 23:24:34' AND r.date_time <= '2024-03-10 23:41:43'
GROUP BY e.name, e.entity_id
ORDER BY total_entity_occurrences DESC
LIMIT 8 -- TOP N entities

/* Filters saga 'hp' */
SELECT e.entity_id, e.name, sum(r.entity_counter) as total_entity_occurrences, sum(r.readers) as total_readers
FROM fact_readers r
    INNER JOIN dim_entity e ON e.entity_id = r.entity_id
    INNER JOIN dim_saga s ON s.saga_id = r.saga_id
WHERE s.name = 'hp'
GROUP BY e.name, e.entity_id
ORDER BY total_entity_occurrences DESC
LIMIT 8 -- TOP N entities