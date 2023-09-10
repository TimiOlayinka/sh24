select Area_of_residence, STI, Gender, 'Total' as Sexual_Orientation, CASE WHEN Age_Group in ('65 +', '65 and over', '65 to 99') THEN '65 +' Else Age_Group END AS Age_Group, `2022` from sh24.dbo.goa_demographic_table
where Gender in ('Men', 'Women', 'Non-Binary / Unknown')
AND Age_Group != 'Total' 
