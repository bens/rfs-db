BEGIN;

CREATE SCHEMA rfs;

CREATE TABLE rfs.raw (
    id         SERIAL8 PRIMARY KEY,
    rfs        JSONB NOT NULL,
    time_added TIMESTAMP WITHOUT TIME ZONE NOT NULL UNIQUE
);

CREATE TABLE rfs.fires (
    id        SERIAL8 PRIMARY KEY,
    permalink TEXT NOT NULL UNIQUE,
    title     TEXT NOT NULL
);

CREATE TABLE rfs.all_updates (
    id              SERIAL8 PRIMARY KEY,
    fire_id         INT8    NOT NULL REFERENCES rfs.fires(id),
    category        TEXT    NOT NULL,
    location        TEXT    NOT NULL,
    council_area    TEXT    NOT NULL,
    status          TEXT    NOT NULL,
    type            TEXT    NOT NULL,
    is_fire         BOOLEAN NOT NULL,
    area_ha         INT     NOT NULL,
    agency          TEXT    NOT NULL,
    last_updated    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    UNIQUE (fire_id, last_updated)
);

CREATE TABLE rfs.mentions (
    raw_id    INT8 REFERENCES rfs.raw (id),
    update_id INT8 REFERENCES rfs.all_updates (id),
    PRIMARY KEY (raw_id, update_id)
);

CREATE VIEW rfs.fire_updates AS
    SELECT id, fire_id, category, location, council_area, status, type,
           area_ha, agency, last_updated
      FROM rfs.all_updates
     WHERE is_fire;

CREATE VIEW rfs.nonfire_updates AS
    SELECT id, fire_id, category, location, council_area, status, type,
           area_ha, agency, last_updated
      FROM rfs.all_updates
     WHERE NOT is_fire;

CREATE FUNCTION rfs.add_new_fires(raw_id INT8) RETURNS VOID AS $$
    WITH
    -- Add fires we haven't seen before and record their ids.
    new_fires(id, permalink) AS (
        INSERT INTO rfs.fires (permalink, title)
             SELECT fs->'properties'->>'guid',
                    fs->'properties'->>'title'
               FROM (SELECT rfs FROM rfs.raw WHERE id = raw_id) x,
            LATERAL JSONB_ARRAY_ELEMENTS(x.rfs->'features') fs
        ON CONFLICT (permalink) DO NOTHING
          RETURNING id, permalink
    ),
    -- Parse all the entries from the raw file, but don't insert yet.
    candidate_updates AS (
        SELECT fires.id AS fire_id,
               fs->'properties'->>'category',
               SUBSTRING(fs->'properties'->>'description' FROM 'LOCATION: ([^<]+) <br />'),
               SUBSTRING(fs->'properties'->>'description' FROM 'COUNCIL AREA: ([^<]+) <br />'),
               SUBSTRING(fs->'properties'->>'description' FROM 'STATUS: ([^<]+) <br />'),
               SUBSTRING(fs->'properties'->>'description' FROM 'TYPE: ([^<]+) <br />'),
               SUBSTRING(fs->'properties'->>'description' FROM 'FIRE: ([^<]+) <br />') = 'Yes',
               SUBSTRING(fs->'properties'->>'description' FROM 'SIZE: ([^<]+) ha')::INT,
               SUBSTRING(fs->'properties'->>'description' FROM 'RESPONSIBLE AGENCY: ([^<]+) <br />'),
               (fs->'properties'->>'pubDate')::TIMESTAMP WITHOUT TIME ZONE AS last_updated
          FROM (SELECT rfs FROM rfs.raw WHERE id = raw_id) x,
       LATERAL JSONB_ARRAY_ELEMENTS(x.rfs->'features') fs
          JOIN (SELECT id, permalink FROM rfs.fires
                       UNION
                SELECT id, permalink FROM new_fires) fires
               ON fires.permalink = fs->'properties'->>'guid'
         ORDER BY (fs->'properties'->>'pubDate')::TIMESTAMP WITHOUT TIME ZONE
    ),
    -- Add updates that we haven't seen, decided by fire_id and update timestamp.
    new_updates(id) AS (
        INSERT INTO rfs.all_updates
                    (fire_id, category,
                     location, council_area, status, type, is_fire, area_ha, agency,
                     last_updated)
             SELECT * FROM candidate_updates
        ON CONFLICT (fire_id, last_updated) DO NOTHING
          RETURNING id
    )
    -- Record all the updates from this raw json, whether they've been newly
    -- added or not.
    INSERT INTO rfs.mentions (raw_id, update_id)
        SELECT raw_id, update_id
          FROM ( SELECT raw_id, new_updates.id
                   FROM new_updates
                        UNION
                 SELECT raw_id, u.id
                   FROM candidate_updates c
                   JOIN rfs.all_updates u
                        ON  u.fire_id = c.fire_id
                        AND u.last_updated = c.last_updated
               ) x(raw_id, update_id)
      ORDER BY 1, 2;
$$ LANGUAGE SQL;

CREATE VIEW rfs.latest_updates AS
   SELECT u.fire_id,
          u.id AS update_id,
          u.last_updated AS last_updated,
          CURRENT_TIMESTAMP - u.last_updated AS age
     FROM (SELECT MAX(r.id ORDER BY r.time_added DESC) FROM rfs.raw r) r(id)
     JOIN rfs.mentions m ON m.raw_id = r.id
     JOIN rfs.all_updates u ON u.id = m.update_id
    ORDER BY 1;

CREATE OR REPLACE FUNCTION rfs.summarise() RETURNS VOID AS $$
DECLARE
    k TEXT; v1 TEXT; v2 TEXT;
    item record;
BEGIN
    -- ###############
    -- ### SUMMARY ###
    -- ###############
    RAISE NOTICE 'Total Area:% ha as at % Sydney time',
      (SELECT TO_CHAR(SUM(u.area_ha), '9,999,999')
        FROM rfs.latest_updates l JOIN rfs.fire_updates u ON u.id = l.update_id),
      (SELECT MAX(time_added AT TIME ZONE 'UTC') AT TIME ZONE 'Australia/Sydney' FROM rfs.raw);
    FOR k, v1, v2 IN (
        SELECT u.status, SUM(u.area_ha), COUNT(1)
          FROM rfs.latest_updates l
          JOIN rfs.fire_updates u ON u.id = l.update_id
      GROUP BY u.status) LOOP
        RAISE NOTICE '%',
            FORMAT(
                '  %-17s%s ha, %2s fires',
                k || ':', TO_CHAR(v1::INT, '999,999'), v2
            );
    END LOOP;
    RAISE NOTICE '';
    -- #################################
    -- ### TOTALS OVER LAST 24 HOURS ###
    -- #################################
    RAISE NOTICE 'Last 24 hours';
    FOR k, v1, v2 IN (
        SELECT (r.time_added AT TIME ZONE 'UTC') AT TIME ZONE 'Australia/Sydney' sydney_time,
               SUM(u.area_ha),
               COUNT(1)
          FROM rfs.raw r
          JOIN rfs.mentions m ON m.raw_id = r.id
          JOIN rfs.fire_updates u ON u.id = m.update_id
         WHERE r.time_added > (CURRENT_TIMESTAMP - '1 day'::INTERVAL)
      GROUP BY r.time_added
      ORDER BY r.time_added DESC) LOOP
        RAISE NOTICE '%',
            FORMAT(
                '  %s:%s ha, %s fires',
                TO_CHAR(k::TIMESTAMP WITH TIME ZONE, 'Mon DDth, HH24:MI'),
                TO_CHAR(v1::INT, '9,999,999'),
                v2
            );
    END LOOP;
    RAISE NOTICE '';
    -- ########################
    -- ### 10 LARGEST FIRES ###
    -- ########################
    RAISE NOTICE '10 Largest Fires';
    FOR item IN (
        SELECT f.title, u.category, u.status, u.council_area, u.area_ha
          FROM rfs.latest_updates l JOIN rfs.fire_updates u ON u.id = l.update_id
          JOIN rfs.fires f ON f.id = u.fire_id
      ORDER BY area_ha DESC
         LIMIT 10) LOOP
        RAISE NOTICE '%',
            FORMAT(
                '  -%s ha %-35s %23s Council (%16s, %13s)',
                TO_CHAR(item.area_ha, '999,999'),
                item.title, item.council_area, item.status, item.category
            );
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;

COMMIT;
