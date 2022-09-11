CREATE VIEW Jan_Users AS
SELECT DISTINCT T0.UserID
FROM emails_sent T0
WHERE T0.MonthID = 1

CREATE VIEW Feb_Users_NoEmail AS
SELECT DISTINCT T0.UserID
FROM modified_listings T0
WHERE T0.MonthID = 2 AND T0.UserID NOT IN (
SELECT DISTINCT T0.UserID
FROM emails_sent T0
WHERE T0.MonthID = 2)

CREATE VIEW Mar_Users AS
SELECT DISTINCT T0.UserID
FROM emails_sent T0
WHERE T0.MonthID = 3

SELECT COUNT(*)
FROM Jan_Users T0
INNER JOIN Feb_Users_NoEmail T1 ON T0.UserID = T1.UserID
INNER JOIN Mar_Users T2 ON T0.UserID = T2.UserID