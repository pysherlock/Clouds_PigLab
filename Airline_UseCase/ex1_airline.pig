%default input 'sample.csv'

-- Load the data
dataset = LOAD '$input' using PigStorage(',') AS (year: int, month: int, day: int, dow: int, 
	dtime: int, sdtime: int, arrtime: int, satime: int, 
	carrier: chararray, fn: int, tn: chararray, 
	etime: int, setime: int, airtime: int, 
	adelay: int, ddelay: int, 
	scode: chararray, dcode: chararray, dist: int, 
	tintime: int, touttime: int, 
	cancel: chararray, cancelcode: chararray, diverted: int, 
	cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);


A = FOREACH dataset GENERATE year, month, day, scode, dcode;
B = GROUP A BY (scode, year, month, day);
C = GROUP A BY (dcode, year, month, day);

inbound = FOREACH B GENERATE (group.year, group.month, group.day) AS date, group.scode, COUNT(A) AS num;
inbound_2 = GROUP inbound BY date;
inbound_result = FOREACH inbound_2{ result = TOP(5, 2, inbound); GENERATE FLATTEN(result);}

outbound = FOREACH C GENERATE (group.year, group.month, group.day) AS date, group.dcode, COUNT(A) AS num;
outbound_2 = GROUP outbound BY date;
outbound_result = FOREACH outbound_2{ result = TOP(5, 2, outbound); GENERATE FLATTEN(result);}


flights = UNION inbound, outbound;
flights_2 = GROUP flights BY (date, scode);
flights_3 = FOREACH flights_2 GENERATE FLATTEN(group), SUM(flights.num);
flights_4 = GROUP flights_3 BY date;
flights_result = FOREACH flights_4{ result = TOP(5, 2, flights_3); GENERATE FLATTEN(result);}
