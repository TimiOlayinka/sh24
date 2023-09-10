Create Table sh24.dbo.ethnics_demographic_table AS
WITH Caty AS (
SELECT 
    Area_of_residence,
    STI,
    CASE 
        WHEN Ethnic_group IN ('Bangladeshi', 'Chinese', 'Indian', 'Pakistani', 'Other Asian', 'Asian') THEN 'Asian'
        WHEN Ethnic_group IN ('Black African', 'Black Caribbean', 'Other Black', 'Black') THEN 'Black'
        WHEN Ethnic_group IN ('White and Asian', 'White and Black African', 'White and Black Caribbean', 'Other Mixed', 'Mixed') THEN 'Mixed'
        --WHEN Ethnic_group Ethnic_group == 'Any other ethnicity' THEN 'Other'
        WHEN Ethnic_group IN ('White British', 'White Irish', 'Other White', 'White') THEN 'White'
        WHEN Ethnic_group IN ('Unknown') THEN 'Unknown'
        WHEN Ethnic_group IN ('Total') THEN 'Total'
        Else 'Other'
    END AS Ethnic_Category,
    Ethnic_group AS Ethnic_Subcategory,
    `2018`,
    `2019`,
    `2020`,
    `2021`,
    `2022`
FROM sh24.staging.Ethnics_Demographic_Line
)
, Final_Ethnics AS (
SELECT 
	*,
   CASE 
       WHEN Area_of_residence = 'England' AND Ethnic_Category = Ethnic_Subcategory THEN 1
       ELSE 0
   END AS Flag
FROM Caty
)

Select 
Area_of_Residence,
STI,
Ethnic_Category,
Ethnic_Subcategory,
Flag,
`2018`,
`2019`,
`2020`,
`2021`,
`2022`
FROM Final_Ethnics
Where Flag = 0 and Ethnic_Category != 'Total'
