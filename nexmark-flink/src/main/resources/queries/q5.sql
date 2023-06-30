-- -------------------------------------------------------------------------------------------------
-- Query 5: Hot Items
-- -------------------------------------------------------------------------------------------------
-- Which auctions have seen the most bids in the last period?
-- Illustrates sliding windows and combiners.
--
-- The original Nexmark Query5 calculate the hot items in the last hour (updated every minute).
-- To make things a bit more dynamic and easier to test we use much shorter windows,
-- i.e. in the last 10 seconds and update every 2 seconds.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE discard_sink (
                            auction  BIGINT,
                            num  BIGINT
) WITH (
    'connector' = 'blackhole'
    );

INSERT INTO discard_sink
SELECT
  Q.auction,
  sum(Q.num)
FROM (
       SELECT AuctionBids.auction, AuctionBids.num,
              COALESCE(AuctionBids.window_start, MaxBids.window_start) as window_start,
              COALESCE(AuctionBids.window_end, MaxBids.window_end) as window_end
       FROM (
              SELECT
                auction,
                count(*) AS num,
                window_start,
                window_end
              FROM TABLE(HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
              GROUP BY
                auction, window_start, window_end
            ) AS AuctionBids
              JOIN (
         SELECT
           max(CountBids.num) AS maxn,
           CountBids.window_start,
           CountBids.window_end
         FROM (
                SELECT
                  count(*) AS num,
                  window_start,
                  window_end
                FROM TABLE(HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
                GROUP BY
                  auction, window_start, window_end
              ) AS CountBids
         GROUP BY CountBids.window_start, CountBids.window_end
       ) AS MaxBids
                   ON AuctionBids.window_start = MaxBids.window_start AND
                      AuctionBids.window_end = MaxBids.window_end AND
                      AuctionBids.num >= MaxBids.maxn
     ) Q
GROUP BY Q.auction, Q.window_start, Q.window_end;