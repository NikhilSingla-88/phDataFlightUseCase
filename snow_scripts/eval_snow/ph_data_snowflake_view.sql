
//Total number of flights by airline and airport on a monthly basis
create or replace view "PHDATAFLIGHTUSECASE"."PUBLIC".v_tot_flight_airp_airl_mon as 
select year,month,b.airline,c.airport,count(1) tot_no_flights
from
(
select year,month,airline,origin_airport as airport  from flights
union all
select year,month,airline,destination_airport as airport from flights ) a,
airlines b,
airports c
where a.airline=b.iata_code
and a.airport=c.iata_code
group by year,month,b.airline,c.airport;

//On-time percentage of each airline for the year 2015
create or replace view "PHDATAFLIGHTUSECASE"."PUBLIC".v_on_time_perc_airl as 
select b.airline,round(on_time_perc,3) on_time_perc from
(select tot.airline,delay_cnt,tot_cnt, (tot_cnt-delay_cnt)*100/tot_cnt as on_time_perc from
(select count(1) delay_cnt,airline from flights where year='2015' and (NVL(ARRIVAL_DELAY,0)<>0  OR NVL(DEPARTURE_DELAY,0)<>0) group by airline) delay,
(select count(1) tot_cnt,airline from flights where year='2015' group by airline) tot where tot.airline=delay.airline ) a,
airlines b where a.airline=b.iata_code order by on_time_perc;

//Airline with max delay. Delay canbe +/- so excluded with 0
create or replace view "PHDATAFLIGHTUSECASE"."PUBLIC".v_max_delay_airl as 
select b.airline from 
(
select airline,ROW_NUMBER() over(order by cnt desc) rn
from
(
select airline,count(1) cnt from flights where NVL(ARRIVAL_DELAY,0)=0  group by airline)
)
a,airlines b where a.airline=b.iata_code and rn=1;


//Cancellation reasons by airport
create or replace view "PHDATAFLIGHTUSECASE"."PUBLIC".v_canc_reason_airp as 
SELECT B.AIRPORT,
CASE WHEN CANCELLATION_REASON='A'then 'Airline/Carrier' 
WHEN CANCELLATION_REASON='B'then 'Weather' 
WHEN CANCELLATION_REASON='C'then 'National Air System' 
WHEN CANCELLATION_REASON='D'then 'Security' 
else CANCELLATION_REASON end as CANCELLATION_REASON FROM (
SELECT CANCELLATION_REASON,AIRPORT FROM
  (select CANCELLATION_REASON, ORIGIN_AIRPORT airport from flights where CANCELLED=1
   union all
   select CANCELLATION_REASON, DESTINATION_AIRPORT airport from flights where CANCELLED=1) group by CANCELLATION_REASON,AIRPORT) A,
AIRPORTS B
WHERE A.AIRPORT=B.IATA_CODE order by airport;	

//Delay reasons by airpor
create or replace view "PHDATAFLIGHTUSECASE"."PUBLIC".v_delay_reason_airp as 
select b.airport,type_of_delay from(
select DESTINATION_AIRPORT airport,
case when NVL(AIR_SYSTEM_DELAY,0)<>0 then 'AIR_SYSTEM_DELAY'
when NVL(SECURITY_DELAY,0)<>0 then 'SECURITY_DELAY'
when NVL(AIRLINE_DELAY,0)<>0 then 'AIRLINE_DELAY'
when NVL(LATE_AIRCRAFT_DELAY,0)<>0 then 'LATE_AIRCRAFT_DELAY'
when NVL(WEATHER_DELAY,0)<>0 then 'WEATHER_DELAY'
end as type_of_delay from flights 
where NVL(ARRIVAL_DELAY,0)<>0
union all
select ORIGIN_AIRPORT airport,
case when NVL(AIR_SYSTEM_DELAY,0)<>0 then 'AIR_SYSTEM_DELAY'
when NVL(SECURITY_DELAY,0)<>0 then 'SECURITY_DELAY'
when NVL(AIRLINE_DELAY,0)<>0 then 'AIRLINE_DELAY'
when NVL(LATE_AIRCRAFT_DELAY,0)<>0 then 'LATE_AIRCRAFT_DELAY'
when NVL(WEATHER_DELAY,0)<>0 then 'WEATHER_DELAY'
end as type_of_delay from flights 
where NVL(DEPARTURE_DELAY,0)<>0) a,
airports b where a.airport=b.iata_code and type_of_delay is not NULL group by b.airport,type_of_delay order by airport;


//Airline with the most unique routes
create or replace view "PHDATAFLIGHTUSECASE"."PUBLIC".v_most_uniq_route_airl as 
select b.airline,t1 _from,t2 _to,cnt from (
select t1,t2,airline,count(1) cnt from 
(select ORIGIN_AIRPORT t1,DESTINATION_AIRPORT t2,airline from flights
union all
select DESTINATION_AIRPORT t1,ORIGIN_AIRPORT t2,airline from flights) group by  t1,t2,airline ) a,
airlines b where a.airline=b.iata_code group by b.airline,t1,t2,cnt order by cnt,airline ;