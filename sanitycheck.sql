=VLOOKUP(A2:A21,Sheet3!A1:B8,2,FALSE)
=IFERROR(VLOOKUP(A$2:A$21,Sheet3!A$1:B$8,2,FALSE),0)

SELECT ID, FROM_UNIXTIME(FIRST_SEEN), FROM_UNIXTIME(LAST_SEEN) FROM SS_RRSET ORDER BY id DESC LIMIT
3;

SELECT ID, FROM_UNIXTIME(FIRST_SEEN) AS FIRST_SEEN, FROM_UNIXTIME(LAST_SEEN) AS LAST_SEEN FROM
SS_RRSET where FIRST_SEEN > UNIX_TIMESTAMP('2018-10-28 11:10:50') ORDER BY id DESC LIMIT 100;

SELECT ID, FROM_UNIXTIME(FIRST_SEEN) AS FIRST_SEEN, FROM_UNIXTIME(LAST_SEEN) AS LAST_SEEN FROM
SS_RRSET where FIRST_SEEN < UNIX_TIMESTAMP(SYSDATE()) ORDER BY id DESC LIMIT 100;

SELECT UNIX_TIMESTAMP(SUBDATE(SYSDATE(), INTERVAL 10 DAY));

SELECT ID, FROM_UNIXTIME(FIRST_SEEN) AS FIRST_SEEN, FROM_UNIXTIME(LAST_SEEN) AS LAST_SEEN FROM
SS_RRSET where FIRST_SEEN > UNIX_TIMESTAMP(SUBDATE(SYSDATE(), INTERVAL 250 DAY)) ORDER BY id DESC
LIMIT 100;

SELECT COUNT(ID) FROM SS_RRSET where LAST_SEEN > UNIX_TIMESTAMP(SUBDATE(SYSDATE(), INTERVAL 7 DAY));

SELECT COUNT(ID) FROM SS_RRSET where LAST_SEEN > UNIX_TIMESTAMP(SUBDATE(SYSDATE(), INTERVAL 30
DAY));

SELECT COUNT(ID) FROM SS_RRSET where LAST_SEEN > UNIX_TIMESTAMP(SUBDATE('2018-10-28 12:00:00',
INTERVAL 7 DAY)); SELECT COUNT(ID) FROM SS_RRSET where LAST_SEEN >
UNIX_TIMESTAMP(SUBDATE('2018-10-28 12:00:00', INTERVAL 30 DAY));

SELECT COUNT(ID) FROM SS_RRSET WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = 2019 AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = 07 AND DAY(FROM_UNIXTIME(FIRST_SEEN)) = 16;

SELECT COUNT(ID) FROM SS_RRSET WHERE YEAR(FROM_UNIXTIME(LAST_SEEN)) = 2019 AND MONTH(FROM_UNIXTIME(LAST_SEEN)) = 07 AND DAY(FROM_UNIXTIME(LAST_SEEN)) = 16 AND FIRST_SEEN!=LAST_SEEN;

select ZONE_ID, COUNT(ID) as SUM from SS_RRSET group by ZONE_ID;

select ZONE_ID, COUNT(ID) as SUM from SS_RRSET group by ZONE_ID INTO OUTFILE '/Users/rahulguna/Desktop/Jun17.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

select ZONE_ID, COUNT(ID),FROM_UNIXTIME(FIRST_SEEN) from SS_RRSET WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = 2018 AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = 04 group by ZONE_ID;

select ZONE_ID, COUNT(ID) from SS_RRSET WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = 2017 AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = 06 group by ZONE_ID INTO OUTFILE '/Users/rahulguna/Desktop/Jun17.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

Query to check number of rows added in SS_RRSET;
SELECT COUNT(ID) FROM SS_RRSET WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = 2019 AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = 07;

Query to check number of rows updated in SS_RRSET;
SELECT COUNT(ID) FROM SS_RRSET WHERE YEAR(FROM_UNIXTIME(LAST_SEEN)) = 2018 AND MONTH(FROM_UNIXTIME(LAST_SEEN)) = 10 AND FIRST_SEEN!=LAST_SEEN;

Query to check number of rows removed in SS_RRSET;
SELECT COUNT(ID) FROM SS_EXP_RRSET WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = 2018 AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = 10;

Query to get average number of rows inserted per zone:
select AVG(SUM) from (select COUNT(ID) AS SUM from SS_RRSET WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = 2018 AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = 10 group by ZONE_ID) MYTABLE;


select CASE WHEN VALUE1 BETWEEN 6 and 10 THEN 'PASS' ELSE 'FAIL' END AS RESULT from (select AVG(SUM) AS VALUE1 from (select COUNT(ID) AS SUM from SS_RRSET WHERE YEAR(FROM_UNIXTIME(FIRST_SEEN)) = 2018 AND MONTH(FROM_UNIXTIME(FIRST_SEEN)) = 10 group by ZONE_ID) AS MYTABLE) AS MYTABLE1;

INSERT INTO SS_RUN_STATS VALUES (249,UNIX_TIMESTAMP(SYSDATE()),UNIX_TIMESTAMP(SYSDATE()));

SELECT SS_RRSET.ID, SS_RRSET.RR_TYPE from SS_RRSET inner join SS_DNSKEY on SS_RRSET.ID = SS_DNSKEY.SET_ID inner join SS_RRSIG on SS_RRSIG.SET_ID = SS_DNSKEY.SET_ID where SS_RRSET.ID=59753;

select ID,RUN_ID,TABLE_NAME,FROM_UNIXTIME(TIMESTAMP),NO_OF_ROWS from SS_TABLE_SANITY_CHECK;

referential integrity:
SELECT SS_EXP_RRSET.ID from SS_EXP_RRSET inner join SS_RRSET_EXP_REL on SS_EXP_RRSET.ID = SS_RRSET_EXP_REL.EXP_SET_ID where SS_EXP_RRSET.ID=%s

CREATE TRIGGER testsanity BEFORE INSERT ON SS_EXP_RRSET
  FOR EACH ROW
  BEGIN
    UPDATE TABLE_SANITY_CHECK SET COUNT = COUNT + 1 WHERE TABLE_NAME = "SS_EXP_RRSET";
  END;


