CREATE TABLE sh24.dbo.goa_population_table AS 
WITH Main_GOA_Table AS (
    SELECT 
    Area_of_residence,
    CASE 
        WHEN Gender_and_sexual_orientation IN ('MSW', 'GBMSM', 'Men') THEN 'Men'
        WHEN Gender_and_sexual_orientation IN ('WSM', 'WSW', 'Women') THEN 'Women'
        ELSE 'Total'
    END AS Gender,
    Gender_and_sexual_orientation as Sexual_Orientation,
	Age_Group_ as Age_Group,
    `2018`,
    `2019`,
    `2020`,
    `2021`,
    `2022`
FROM sh24.staging.GOA_Population_Line
), 
Total_SO AS (
	SELECT 
        Area_of_residence,
        Gender,
        Sexual_Orientation,
        Age_Group,
        `2018`,
        `2019`,
        `2020`,
        `2021`,
        `2022`
    FROM Main_GOA_Table
	WHERE Gender = Sexual_Orientation
	AND Age_group != 'Total'
)

, Unknown_SO AS (
    SELECT 
        Area_of_residence,
        Gender,
        'Unknown' AS Sexual_Orientation,
        Age_Group,
        SUM(`2018`) AS Total2018,
        SUM(`2019`) AS Total2019,
        SUM(`2020`) AS Total2020,
        SUM(`2021`) AS Total2021,
        SUM(`2022`) AS Total2022
    FROM Main_GOA_Table
    WHERE Gender != Sexual_Orientation
	AND Age_group != 'Total'
    GROUP BY 
        Area_of_residence,
        Gender,
        Age_Group
), Final_Unknown_SO AS (SELECT 
    g1.Area_of_residence,
    g1.Gender,
    g2.Sexual_Orientation, 
    g1.Age_Group,
    COALESCE(g1.`2018`, 0) - COALESCE(g2.Total2018, 0) AS `2018`,
    COALESCE(g1.`2019`, 0) - COALESCE(g2.Total2019, 0) AS `2019`,
    COALESCE(g1.`2020`, 0) - COALESCE(g2.Total2020, 0) AS `2020`,
    COALESCE(g1.`2021`, 0) - COALESCE(g2.Total2021, 0) AS `2021`,
    COALESCE(g1.`2022`, 0) - COALESCE(g2.Total2022, 0) AS `2022`
FROM Total_SO g1
LEFT JOIN Unknown_SO g2
    ON g1.Area_of_residence = g2.Area_of_residence
    AND g1.Age_Group = g2.Age_Group
    AND g1.Gender = g2.Gender)

, Union_Unknown_SO AS (Select * FROM Main_GOA_Table
UNION ALL
Select * FROM Final_Unknown_SO
where Sexual_Orientation IS NOT NULL
)
, Total_Gender AS (
	SELECT 
        Area_of_residence,
        Gender,
        Sexual_Orientation,
        Age_Group,
        `2018`,
        `2019`,
        `2020`,
        `2021`,
        `2022`
    FROM Union_Unknown_SO
	WHERE 1=1
	AND Gender = 'Total'
)
, Unknown_Gender AS (
    SELECT 
        Area_of_residence,
        'Unknown' AS Gender,
        'Unknown' AS Sexual_Orientation,
        Age_Group,
        SUM(`2018`) AS Total2018,
        SUM(`2019`) AS Total2019,
        SUM(`2020`) AS Total2020,
        SUM(`2021`) AS Total2021,
        SUM(`2022`) AS Total2022
    FROM Union_Unknown_SO
    WHERE Gender NOT IN ('Total', 'Unknown')
	AND Gender = Sexual_Orientation
    GROUP BY 
        Area_of_residence,
        --Gender,
        Age_Group
		--Sexual_Orientation
)

, Final_Unkown_Gender AS (
    SELECT 
        g1.Area_of_residence,
        g2.Gender,
        g2.Sexual_Orientation, 
        g1.Age_Group,
        COALESCE(g1.`2018`, 0) - COALESCE(g2.Total2018, 0) AS `2018`,
        COALESCE(g1.`2019`, 0) - COALESCE(g2.Total2019, 0) AS `2019`,
        COALESCE(g1.`2020`, 0) - COALESCE(g2.Total2020, 0) AS `2020`,
        COALESCE(g1.`2021`, 0) - COALESCE(g2.Total2021, 0) AS `2021`,
        COALESCE(g1.`2022`, 0) - COALESCE(g2.Total2022, 0) AS `2022`
    FROM Total_Gender g1
    LEFT JOIN Unknown_Gender g2
        ON g1.Area_of_residence = g2.Area_of_residence
        AND g1.Age_Group = g2.Age_Group
)
, Final_Table AS (
Select * FROM Union_Unknown_SO
Union ALL
Select * FROM Final_Unkown_Gender
)


Select 
    * 
FROM Final_Table
ORDER BY Area_of_residence,  Gender, Sexual_Orientation 
