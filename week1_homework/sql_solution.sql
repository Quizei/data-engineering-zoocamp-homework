
--Qno 3: ans="10+ miles"	"  35,189"
/*
"1~3 miles"	" 198,924"
"3~7 miles"	" 109,603"
"7~10 miles"	"  27,678"
"Up to 1 mile"	" 104,802"
*/
    select
        case
            when trip_distance <= 1 then 'Up to 1 mile'
            when trip_distance > 1 and trip_distance <= 3 then '1~3 miles'
            when trip_distance > 3 and trip_distance <= 7 then '3~7 miles'
            when trip_distance > 7 and trip_distance <= 10 then '7~10 miles'
            else '10+ miles'
        end as segment,
        to_char(count(1), '999,999') as num_trips
    from
        green_taxi_trips
    where
        lpep_pickup_datetime >= '2019-10-01'
        and lpep_pickup_datetime < '2019-11-01'
        and lpep_dropoff_datetime >= '2019-10-01'
        and lpep_dropoff_datetime < '2019-11-01'
    group by
        segment


--Qno 4: ans = 2019-10-31
    select 
        date(lpep_pickup_datetime) , max(trip_distance) 
    from 
        green_tripdata_2019_10 
    group by 1 
    order by 2 
    desc limit 1


--Qno 5: ans = "East Harlem North" , "East Harlem South" , "Morningside Heights"
    select tz.zone ,round(sum(total_amount)::numeric , 1) as grand_total from green_tripdata_2019_10 as gt left join
    taxi_zones as tz 
    on 
    gt.pulocationid = tz.locationid
    where date(lpep_pickup_datetime)= '2019-10-18'
    group by 1 
    having sum(total_amount) > 13000
    order by 2 desc
    limit 3


--Qno 6: ans= "JFK Airport"
    SELECT 
        tz_2.zone, 
        MAX(gt.tip_amount) AS largest_tip
    FROM 
        green_tripdata_2019_10 AS gt
    LEFT JOIN 
        taxi_zones AS tz 
        ON gt.pulocationid = tz.locationid
    LEFT JOIN 
        taxi_zones AS tz_2 
        ON gt.dolocationid = tz_2.locationid
    WHERE 
        EXTRACT(YEAR FROM gt.lpep_pickup_datetime::timestamp) = 2019
        AND EXTRACT(MONTH FROM gt.lpep_pickup_datetime::timestamp) = 10
        AND tz.zone = 'East Harlem North'
    GROUP BY 
        tz_2.zone
    ORDER BY 
        largest_tip DESC
    LIMIT 1;
