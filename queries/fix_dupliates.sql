BEGIN;
DELETE FROM foerderdatenbankdumpbackend.foerderdatenbankdumpbackend a
USING (
    SELECT MAX(_dlt_load_id) as max_load_id, id_hash as key
    FROM foerderdatenbankdumpbackend.foerderdatenbankdumpbackend
    GROUP BY id_hash, checksum
    HAVING COUNT(*) > 1
) b
WHERE a.id_hash = b.key
AND a.checksum = (
    SELECT checksum
    FROM foerderdatenbankdumpbackend.foerderdatenbankdumpbackend
    WHERE id_hash = b.key
    LIMIT 1
)
AND a._dlt_load_id <> b.max_load_id;
COMMIT;
