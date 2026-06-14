/*Star schema = standard for data warehouse pattern where one central table holds all
 raw events (the "facts" table)and smaller tables hold descriptive info (the "dimension" tables).
*/

-- DIMENSION TABLES
/*types of objects YOLO can detect (person, car, dog, etc.). Store oce here and reference by class_id
rather than writing the word "person" into every detection*/
CREATE TABLE IF NOT EXISTS dim_class (
    class_id    SERIAL PRIMARY KEY,  --SERIAL = auto-incrementing integer columns, common for primary keys
    class_name  VARCHAR(100) NOT NULL UNIQUE,
    category    VARCHAR(50)
);

/*breaks timestamp into parts so can query based on time unit*/
CREATE TABLE IF NOT EXISTS dim_time (
    time_id     SERIAL PRIMARY KEY,
    full_ts     TIMESTAMP NOT NULL UNIQUE,
    hour        INT,
    day         INT,
    month       INT,
    year        INT,
    day_of_week VARCHAR(10)
);

/*tracks where stream came from. This project only has one 'coc128-stream', but real
would have multiple cameras or feeds. Reduces need to hardcode stream names everywhere*/
CREATE TABLE IF NOT EXISTS dim_source (
    source_id   SERIAL PRIMARY KEY,
    stream_name VARCHAR(100) NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- FACT TABLE/RAW EVENTS
/*detection_events is the core of the schema. Every single object detection gets one row here. It records:

    frame_id — which image frame the detection came from
    detected_at — when it happened
    class_id — what was detected (foreign key linking to dim_class)
    source_id — which stream it came from (foreign key linking to dim_source)
    confidence — how sure the model was (0.0 to 1.0)
    bbox_x/y/w/h — the bounding box coordinates telling you where in the frame the object appeared

The foreign keys (REFERENCES dim_class, REFERENCES dim_source) enforce data integrity — you can't insert 
a detection with a class or source that doesn't exist in the dimension tables.*/

CREATE TABLE IF NOT EXISTS detection_events (
    detection_id    BIGSERIAL PRIMARY KEY,
    frame_id        VARCHAR(100) NOT NULL,
    detected_at     TIMESTAMP NOT NULL,
    class_id        INT REFERENCES dim_class(class_id),
    source_id       INT REFERENCES dim_source(source_id),
    confidence      FLOAT NOT NULL,
    bbox_x          FLOAT,
    bbox_y          FLOAT,
    bbox_w          FLOAT,
    bbox_h          FLOAT
);

-- AGGREGATE / SUMMARY TABLE (data warehouse output)
/*output of Airflow ETL job. Rather than querying millions of raw detection rows every time someone wants 
a report, Airflow precomputes hourly totals and stores them here. A query like "how many people were detected 
between 2pm and 3pm?" runs instantly against this table instead of scanning the entire detection_events table.
This is a core data warehousing concept — separating raw data from aggregated reporting data.*/
CREATE TABLE IF NOT EXISTS detection_summary (
    summary_id      BIGSERIAL PRIMARY KEY,
    hour_bucket     TIMESTAMP NOT NULL,
    class_name      VARCHAR(100),
    total_count     INT,
    avg_confidence  FLOAT,
    computed_at     TIMESTAMP DEFAULT NOW()
);

-- SEED DIMENSION DATA
/*adds the coco128-stream entry so that when the producer starts sending frames labeled with that stream 
name, the foreign key lookup in detection_events already has something to point to. Without this, the first 
insert would fail.*/
INSERT INTO dim_source (stream_name) VALUES ('coco128-stream')
ON CONFLICT DO NOTHING;

/*preloads the 10 object classes YOLO commonly detects. Same reason. The consumer looks up class_id by name 
when writing detections, so the names need to already exist.*/
INSERT INTO dim_class (class_name, category) VALUES
    ('person',     'human'),
    ('car',        'vehicle'),
    ('truck',      'vehicle'),
    ('bicycle',    'vehicle'),
    ('motorcycle', 'vehicle'),
    ('bus',        'vehicle'),
    ('dog',        'animal'),
    ('cat',        'animal'),
    ('chair',      'furniture'),
    ('laptop',     'electronics')
ON CONFLICT DO NOTHING;  --if you restart Postgres and it tries to run this file again, it won't crash trying to insert duplicates.