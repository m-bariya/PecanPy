/* SKIPPED COLUMNS (due to being free text):
pv_system_size, pv_system_reason, pv_system_features_liked, pv_system_features_for_improvement,
pv_system_features_surprising, pv_system_common_questions, pv_system_common_response,
pv_system_common_surprises, pv_owner_response, retrofits_detail skipped, compressor1_*,
compressor2_*, compressor3_*, electronic_devices_on_other, thermostats_brand, programmable_thermostat_difficulty skipped */
/* NOTES:
ethnicity should be a single categorical column, but some surveys have more than one ethnicity selected */
/* For definition in Pandas DataFrame:
ORDERED CATEGORICAL fields can be passed as they are to a Decision Tree Classifier, but should probably be encoded using OneHotEnccoder for other algorithms.
UNORDERED CATEGORICAL fields should *probably* be encoded using OneHotEncoder either way. Using OHE with random forests can possibly induce bias.
survey_status_p: boolean
foundation_type_p: unordered categorical
home_non_p: boolean
home_mon_p: boolean
home_tue_p: boolean
home_wed_p: boolean
home_thur_p: boolean
home_fri_p: boolean
asian_pacific_islander_p: boolean
black_p: boolean
white_p: boolean
hispanic_p: boolean
alaska_p: boolean
other_p: boolean
decline_p: boolean
residents_0612_p: integer
residents_1318_p: integer
residents_1924_p: integer
residents_2534_p: integer
residents_3549_p: integer
residents_5064_p: integer
residents_65ol_p: integer
education_level_p: ordered categorical
income_level_p: ordered categorical
pv_present_p: boolean
pv_satisfied_p: ordered categorical
pvneg_expense_p: boolean
pvneg_uncertain_p: boolean
pvneg_ugly_p: boolean
pvneg_resale_p: boolean
pvneg_none_p: boolean
pvneg_othr_p: boolean
pvpos_indep_p: boolean
pvpos_green_p: boolean
pvpos_rate_p: boolean
pvpos_othr_p: boolean
pvpos_none_p: boolean
retrofit_p: boolean
irrigation_p: boolean
ceiling_fans_p: boolean
hvac_p: ordered categorical - ordered from highest to lowest energy intensity
compressors_p: integer
temp_summer_weekday_workday_p: integer
temp_summer_weekday_morning_p: integer
temp_summer_weekday_evening_p: integer
temp_summer_sleeping_hours_hours_p: integer
temp_summer_weekend_hours_p: integer
temp_winter_weekday_workday_p: integer
temp_winter_weekday_morning_p: integer
temp_winter_weekday_evening_p: integer
temp_winter_sleeping_hours_hours_p: integer
temp_winter_weekend_hours_p: integer
programmed_p: boolean
ac_service_p: boolean
cable_box_num_p: integer
dvr_num_p: integer
wifi_num_p: integer
game_num_P: integer
*/


select dataid, (case when status ='Complete' then 1 else 0 end) as survey_status_P,
(case when foundation_pier_beam='Pier and beam' and foundation_slab='Slab' then 3 when foundation_pier_beam='Pier and beam' then 2 when foundation_slab='Slab' then 1 else NULL end) as foundation_type_P,
(case when spend_time_at_home_none = 'none' then 1 else 0 end) as home_non_P, 
(case when spend_time_at_home_monday = 'Monday' then 1 else 0 end) as home_mon_P, 
(case when spend_time_at_home_tuesday = 'Tuesday' then 1 else 0 end) as home_tue_P, 
(case when spend_time_at_home_wednesday = 'Wednesday' then 1 else 0 end) as home_wed_P, 
(case when spend_time_at_home_thursday = 'Thursday' then 1 else 0 end) as home_thur_P, 
(case when spend_time_at_home_friday = 'Friday' then 1 else 0 end) as home_fri_P, 
(case when ethnicity_asian_pacific_islander = 'Asian/Pacific Islander' then 1 else 0 end) as asian_pacific_islander_P, 
(case when ethnicity_black_african_american = 'Black/African-American' then 1 else 0 end) as black_P, 
(case when ethnicity_caucasian_other_than_hispanic_or_latino = 'Caucasian' then 1 else 0 end) as white_P, 
(case when ethnicity_hispanic_or_latino = 'Hispanic' then 1 else 0 end) as hispanic_P, 
(case when ethnicity_native_american_alaska_native = 'Native American/Alaska Native' then 1 else 0 end) as alaska_P, 
(case when ethnicity_other = 'Other' then 1 else 0 end) as other_P, 
(case when ethnicity_decline = 'Decline to Respond' then 1 else 0 end) as decline_P, 
(case when residents_under_5 = '' then 0 when residents_under_5 = '5 or more' then 5 else cast(residents_under_5 as int) end) as residents_un05_P, 
(case when residents_6_to_12 = '' then 0 when residents_6_to_12 = '5 or more' then 5 else cast(residents_6_to_12 as int) end) as residents_0612_P, 
(case when residents_13_to_18 = '' then 0 when residents_13_to_18 = '5 or more' then 5 else cast(residents_13_to_18 as int) end) as residents_1318_P, 
(case when residents_19_to_24 = '' then 0 when residents_19_to_24 = '5 or more' then 5 else cast(residents_19_to_24 as int) end) as residents_1924_P, 
(case when residents_25_to_34 = '' then 0 when residents_25_to_34 = '5 or more' then 5 else cast(residents_25_to_34 as int) end) as residents_2534_P, 
(case when residents_35_to_49 = '' then 0 when residents_35_to_49 = '5 or more' then 5 else cast(residents_35_to_49 as int) end) as residents_3549_P, 
(case when residents_50_to_64 = '' then 0 when residents_50_to_64 = '5 or more' then 5 else cast(residents_50_to_64 as int) end) as residents_5064_P, 
(case when residents_65_and_older = '' then 0 when residents_65_and_older = '5 or more' then 5 else cast(residents_65_and_older as int) end) as residents_65ol_P, 
(case when education_level = 'High School graduate' then 1 when education_level = 'Some college/trade/vocational school' then 2 when education_level = 'College graduate' then 3 when education_level = 'Postgraduate degree' then 4 else NULL end) as education_level_P, 
(case when total_annual_income='"""Less than $10,000"""' then 1 when total_annual_income='"""$10,000 - $19,999"""' then 2 when total_annual_income='"""$20,000 - $34,999"""' then 3 when total_annual_income='"""$35,000 - $49,999"""' then 4 when total_annual_income='"""$50,000 - $74,999"""' then 5 when total_annual_income='"""$75,000 - $99,999"""' then 6 when total_annual_income='"""$100,000 - $149,999"""' then 7 when total_annual_income='"""$150,000 - $299,000"""' then 8 when total_annual_income='"""$300,000 - $1,000,000"""' then 9 when total_annual_income='"""more than $1,000,000"""' then 10 when total_annual_income='' then NULL end) as income_level_P, 
(case when pv_system_own='Yes' then 1 else 0 end) as pv_present_P, 
(case when pv_system_own != 'Yes' then 0 when pv_system_satisfied = 'Very dissatisfied' then 1 when pv_system_satisfied = 'Somewhat dissatisfied' then 2 when pv_system_satisfied = 'Neutral' then 3 when pv_system_satisfied = 'Somewhat satisfied' then 4 when pv_system_satisfied = 'Very' then 5 else NULL end) as pv_satisfied_P, 
(case when pv_neg_factors_too_expensive = 'Too expensive' then 1 else 0 end) as pvneg_expense_P, 
(case when pv_neg_factors_uncertainty = 'Not sure how much I would benefit' then 1 else 0 end) as pvneg_uncertain_P, 
(case when replace(pv_neg_factors_ugly,'''','') = '"""Dont like the way they look"""' then 1 else 0 end) as pvneg_ugly_P, 
(case when replace(pv_neg_factors_resale_value_concerns,'''','') = '"""Concerned with how it might affect my homes resale value"""' then 1 else 0 end) as pvneg_resale_P, 
(case when pv_neg_factors_none = 'None' then 1 else 0 end) as pvneg_none_P, 
(case when pv_neg_factors_other = 'other' then 1 else 0 end) as pvneg_othr_P, 
(case when pv_pos_factors_independence = 'Independence from the utility' then 1 else 0 end) as pvpos_indep_P, 
(case when pv_pos_factors_no_emissions = 'Emission-free electricity' then 1 else 0 end) as pvpos_green_P, 
(case when pv_pos_rate_increase_protection = 'Protection against future utility rate increases' then 1 else 0 end) as pvpos_rate_P, 
(case when pv_pos_factors_other = 'other' then 1 else 0 end) as pvpos_othr_P, 
(case when pv_pos_factors_none = 'None' then 1 else 0 end) as pvpos_none_P, 
(case when retrofits ='Yes' then 1 else 0 end) as retrofit_P, 
(case when irrigation_system ='Yes' then 1 else 0 end) as irrigation_P, 
(case when ceiling_fans_count = '' then 0 else cast(ceiling_fans_count as int) end) as ceiling_fans_P, 
(case when hvac_window_ac = 'Window unit' then 1
when hvac_ductless = 'Mini-split system' then 2
when hvac_central_air_gas_furnace='Split system with gas furnace' then 3
when hvac_central_air_electric_furnace='Central air system with electric heating' then 4
when hvac_twoway_heat_pump='Heat Pump' then 5
when hvac_geothermal_heat_pump='Geothermal heat pump' then 6
when hvac_no_ac='No air conditioning' then 7
when replace(hvac_dont_know,'''','') = '"""I dont know"""' then 0 else 0 end) as hvac_P, 
(case when compressors_count = '' then 0 else cast(compressors_count as int) end) as compressors_P, 
(case when (temp_summer_weekday_workday !~ '\d+') then NULL else cast(temp_summer_weekday_workday as int) end) as temp_summer_weekday_workday_p, 
(case when (temp_summer_weekday_morning !~ '\d+') then NULL else cast(temp_summer_weekday_morning as int) end) as temp_summer_weekday_morning_p, 
(case when (temp_summer_weekday_evening !~ '\d+') then NULL else cast(temp_summer_weekday_evening as int) end) as temp_summer_weekday_evening_p, 
(case when (temp_summer_sleeping_hours_hours !~ '\d+') then NULL else cast(temp_summer_sleeping_hours_hours as int) end) as temp_summer_sleeping_hours_hours_p, 
(case when (temp_summer_weekend_hours !~ '\d+') then NULL else cast(temp_summer_weekend_hours as int) end) as temp_summer_weekend_hours_p, 
(case when (temp_winter_weekday_workday !~ '\d+') then NULL else cast(temp_winter_weekday_workday as int) end) as temp_winter_weekday_workday_p, 
(case when (temp_winter_weekday_morning !~ '\d+') then NULL else cast(temp_winter_weekday_morning as int) end) as temp_winter_weekday_morning_p, 
(case when (temp_winter_weekday_evening !~ '\d+') then NULL else cast(temp_winter_weekday_evening as int) end) as temp_winter_weekday_evening_p, 
(case when (temp_winter_sleeping_hours_hours !~ '\d+') then NULL else cast(temp_winter_sleeping_hours_hours as int) end) as temp_winter_sleeping_hours_hours_p, 
(case when (temp_winter_weekend_hours !~ '\d+') then NULL else cast(temp_winter_weekend_hours as int) end) as temp_winter_weekend_hours_p, 
(case when programmable_thermostat_currently_programmed = '' then NULL when programmable_thermostat_currently_programmed = 'Yes' then 1 when programmable_thermostat_currently_programmed = 'No' then 0 else 2 end) as programmed_P, 
(case when ac_service_package ='Yes' then 1 else 0 end) as ac_service_P, 
(case when electronic_devices_cable_box_number = 'None' then 0 when electronic_devices_cable_box_number = '' then 0 when electronic_devices_cable_box_number = '5 or more' then 5 else cast(electronic_devices_cable_box_number as int) end) as cable_box_num_P, 
(case when electronic_devices_dvr_number = 'None' then 0 when electronic_devices_dvr_number = '' then 0 when electronic_devices_dvr_number = '5 or more' then 5 else cast(electronic_devices_dvr_number as int) end) as dvr_num_P, 
(case when electronic_devices_wifi_number = 'None' then 0 when electronic_devices_wifi_number = '' then 0 when electronic_devices_wifi_number = '5 or more' then 5 else cast(electronic_devices_wifi_number as int) end) as wifi_num_P, 
(case when electronic_devices_game_system_number = 'None' then 0 when electronic_devices_game_system_number = '' then 0 when electronic_devices_game_system_number = '5 or more' then 5 else cast(electronic_devices_game_system_number as int) end) as game_num_P
from SCHEMA.survey_2014_all_participants;



