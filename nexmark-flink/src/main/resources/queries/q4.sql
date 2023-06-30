-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category
-- -------------------------------------------------------------------------------------------------
-- Select the average of the wining bid prices for all auctions in each category.
-- Illustrates complex join and aggregation.
-- -------------------------------------------------------------------------------------------------

-- TODO: streaming join doesn't support rowtime attribute in input, this should be fixed by FLINK-18651.
--  As a workaround, we re-create a new view without rowtime attribute for now.
CREATE TABLE discard_sink (
                              id BIGINT,
                              final BIGINT
) WITH (
      'connector' = 'blackhole'
      );

INSERT INTO discard_sink
SELECT
    Q.category,
    AVG(Q.final)
FROM (
         SELECT MAX(B.price) AS final, A.category,
                COALESCE(A.window_start, B.window_start) as window_start,
                COALESCE(A.window_end, B.window_end) as window_end
         FROM (
                  SELECT * FROM TABLE(TUMBLE(TABLE auction, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
              ) A
                  JOIN (
             SELECT * FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND))
         ) B
                       ON A.id = B.auction
                           AND A.window_start = B.window_start
                           AND A.window_end = B.window_end
                           AND B.dateTime BETWEEN A.dateTime AND A.expires
         GROUP BY A.id, A.category, COALESCE(A.window_start, B.window_start), COALESCE(A.window_end, B.window_end)
     ) Q
GROUP BY Q.category, Q.window_start, Q.window_end;