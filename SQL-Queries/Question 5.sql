SELECT COUNT(*)
FROM(
SELECT DISTINCT T0.UserID
FROM emails_sent T0
INNER JOIN modified_listings T1 ON T0.UserID = T1.UserID
AND T1.MonthID = 1
WHERE T0.MonthID = 1 AND T0.UserID NOT IN (
SELECT DISTINCT T0.UserID
FROM emails_sent T0
WHERE T0.MonthID = 1 AND T0.EmailOpened=1)
AND T1.ModifiedListing = 1) Z0