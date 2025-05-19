library(purrr)
library(tidyverse)
library(bigrquery)
library(dplyr)
library(gtsummary)
library(lpSolve)
library(irr)
library(ggplot2)
library(cardx)
library(broom.helpers)
library(stringr)

## 1 SQL code to grab data from source tables

# Function to grab data from source tables
retrieve_data <- function(sql=NULL, concept_ids=NULL, int_type="integer") {
  if (!is.null(concept_ids)) {
    concept_id_str <- paste(concept_ids, collapse = ", ")
    sql <- paste(sql, concept_id_str, "
                 )", sep="")
  }
  
  bq_table_download(
    bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), sql, billing = Sys.getenv("GOOGLE_PROJECT")),
    bigint = int_type
  )
}

# Survey basics
basics_sql <- paste("
    SELECT
        answer.person_id,
        answer.survey_datetime,
        answer.survey,
        answer.question_concept_id,
        answer.question,
        answer.answer_concept_id,
        answer.answer,
        answer.survey_version_concept_id,
        answer.survey_version_name  
    FROM
        `ds_survey` answer   
    WHERE
        (
            question_concept_id IN (SELECT
                DISTINCT concept_id                         
            FROM
                `cb_criteria` c                         
            JOIN
                (SELECT
                    CAST(cr.id as string) AS id                               
                FROM
                    `cb_criteria` cr                               
                WHERE
                    concept_id IN (1586134)                               
                    AND domain_id = 'SURVEY') a 
                    ON (c.path like CONCAT('%', a.id, '.%'))                         
            WHERE
                domain_id = 'SURVEY'                         
                AND type = 'PPI'                         
                AND subtype = 'QUESTION')
        )", sep="")

basics_data <- retrieve_data(basics_sql)

# Survey lifestyle
lifestyle_sql <- paste("
    SELECT
        answer.person_id,
        answer.survey_datetime,
        answer.survey,
        answer.question_concept_id,
        answer.question,
        answer.answer_concept_id,
        answer.answer,
        answer.survey_version_concept_id,
        answer.survey_version_name  
    FROM
        `ds_survey` answer   
    WHERE
        (
            question_concept_id IN (SELECT
                DISTINCT concept_id                         
            FROM
                `cb_criteria` c                         
            JOIN
                (SELECT
                    CAST(cr.id as string) AS id                               
                FROM
                    `cb_criteria` cr                               
                WHERE
                    concept_id IN (1585855)                               
                    AND domain_id = 'SURVEY') a 
                    ON (c.path like CONCAT('%', a.id, '.%'))                         
            WHERE
                domain_id = 'SURVEY'                         
                AND type = 'PPI'                         
                AND subtype = 'QUESTION')
        )", sep="")

lifestyle_data <- retrieve_data(lifestyle_sql)

# Cohort with EHR data (person-level demographics) and lifestyle survey
EHR_linked_sql <- paste("
    SELECT
        person.person_id,
        person.gender_concept_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        person.race_concept_id,
        p_race_concept.concept_name as race,
        person.ethnicity_concept_id,
        p_ethnicity_concept.concept_name as ethnicity,
        person.sex_at_birth_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `concept` p_race_concept 
            ON person.race_concept_id = p_race_concept.concept_id 
    LEFT JOIN
        `concept` p_ethnicity_concept 
            ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id 
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                age_at_consent BETWEEN 18 AND 124 ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_ehr_data = 1 ) 
            AND cb_search_person.person_id IN (SELECT
                criteria.person_id 
            FROM
                (SELECT
                    DISTINCT person_id, entry_date, concept_id 
                FROM
                    `cb_search_all_events` 
                WHERE
                    (concept_id IN(SELECT
                        DISTINCT c.concept_id 
                    FROM
                        `cb_criteria` c 
                    JOIN
                        (SELECT
                            CAST(cr.id as string) AS id       
                        FROM
                            `cb_criteria` cr       
                        WHERE
                            concept_id IN (1585855)       
                            AND full_text LIKE '%_rank1]%'      ) a 
                            ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                            OR c.path LIKE CONCAT('%.', a.id) 
                            OR c.path LIKE CONCAT(a.id, '.%') 
                            OR c.path = a.id) 
                    WHERE
                        is_standard = 0 
                        AND is_selectable = 1) 
                    AND is_standard = 0 )) criteria ) )", sep="")

EHR_linked_data <- retrieve_data(EHR_linked_sql)

# Observation fact table for smoking facts
tobacco_observation_sql <- paste("
WITH TobaccoObservations AS ( 
    SELECT 
        person_id,
        observation_date,
        observation_concept_id,
        value_as_number,
        value_as_concept_id
    FROM `observation` observation
    WHERE observation_concept_id IN (
         36305168, 903654, 4005823, 903653, 903661, 43054909, 903667, 903666, 
         903664, 903656, 903651, 37017812, 903657, 903662, 3004518, 903660, 
         40770349, 903659, 903655, 903652, 903665, 903668, 36303694, 903663, 
         44786668, 903658, 4038739, 903669, 21494887, 36303803
    )
    AND observation_source_concept_id NOT IN (
                          1585857, 1586162, 1586163, 903064, 1586173, 1586159,
                          1586160, 903063, 1585864, 1585865, 1585866, 1586165,
                          1586181, 1585873, 1586157, 1586158, 1585867, 1585860,
                          1586189
                      )
    
),
SmokerIndicators AS (
    SELECT 
        person_id,
        observation_date,
        observation_concept_id,
        value_as_number,
        value_as_concept_id,

        -- Current smoker classification
        CASE 
            WHEN 
                (
                    observation_concept_id IN (903652, 43054909, 903654, 903657, 36305168, 903666, 903655)
                    AND value_as_concept_id IN (
                        4276526, 42709996, 4005823, 45877994, 4298794, 37395605, 4246415, 4044775, 
                        37017610, 4218741, 762498, 4218917, 36716478, 4209585, 4216174, 762499, 
                        4215409, 4044778, 4141787, 4034855, 4190573, 4058137, 4209423, 37203948, 
                        4042037, 4144273, 4052029, 4038738, 4052030, 4145798, 4044777, 36716991, 
                        437264, 36716473, 4041511, 45884037, 45881517, 45878118, 45884038, 4188539,
                        45884084
                    )
                ) 
                OR (observation_concept_id IN (4005823, 903655, 903658, 903659, 903660, 903661, 903662, 903664, 903668, 903665, 903667, 903663, 903669, 903657))
                OR (observation_concept_id IN (3004518, 36303803) AND value_as_number > 0)
            THEN 1
            ELSE 0
        END AS current_smoker,

        -- Former smoker classification
        CASE 
            WHEN 
                (
                    observation_concept_id IN (903652, 43054909, 903654, 36305168, 903666, 903651)
                    AND value_as_concept_id IN (
                        4310250, 4052032, 45765917, 764567, 4052464, 4092281, 4148416, 4052949, 
                        4043059, 4145798, 4141782, 4141783, 4197592, 4141784, 46270534, 45883458, 
                        36307819
                    )
                )
                OR observation_concept_id  IN (44786668)
            THEN 1
            ELSE 0
        END AS former_smoker,

        -- Non-smoker classification
        CASE 
            WHEN 
                (
                    observation_concept_id IN (903652, 43054909, 903654, 36305168, 903666, 903651)
                    AND value_as_concept_id IN (
                        4144272, 45878245, 45765920, 4222303, 45876662, 4184633, 37017812, 4030580, 
                        4227889, 46273081, 4038739, 45879404, 45877986, 764151, 36308879
                    )
                )
                OR observation_concept_id IN (903656, 903653, 37017812, 4038739)
            THEN 1
            ELSE 0
        END AS non_smoker
    FROM TobaccoObservations
)
SELECT 
    person_id,
    observation_date,
    observation_concept_id,
    value_as_number,
    value_as_concept_id,
    MAX(current_smoker) AS current_smoker,
    MAX(former_smoker) AS former_smoker,
    MAX(non_smoker) AS non_smoker
FROM SmokerIndicators
GROUP BY person_id, observation_date, observation_concept_id, value_as_number, value_as_concept_id
ORDER BY person_id, observation_date", sep="")

tobacco_data <- retrieve_data(tobacco_observation_sql)

# Standardized pre-survey visit data available in EHR
visit_observation_sql <- paste("
WITH DescendantConcepts AS (
  SELECT 
    ca.descendant_concept_id,
    CASE
      WHEN ca.ancestor_concept_id = 9202 THEN 'Outpatient_Visit'
      WHEN ca.ancestor_concept_id = 9201 THEN 'Inpatient_Visit'
      WHEN ca.ancestor_concept_id = 9203 THEN 'ER_Visit'
      WHEN ca.ancestor_concept_id = 262 THEN 'ER_Inpatient_Visit'
      WHEN ca.ancestor_concept_id = 42898160 THEN 'Non_Hospital_Visit'
      WHEN ca.ancestor_concept_id = 581476 THEN 'Home_Visit'
      WHEN ca.ancestor_concept_id = 722455 THEN 'Telehealth_Visit'
      WHEN ca.ancestor_concept_id = 581458 THEN 'Pharmacy_Visit'
      WHEN ca.ancestor_concept_id = 32036 THEN 'Laboratory_Visit'
      WHEN ca.ancestor_concept_id = 581478 THEN 'Ambulance_Visit'
      WHEN ca.ancestor_concept_id = 38004193 THEN 'Case_Management_Visit'
      ELSE 'Other'
    END AS visit_type
  FROM `concept_ancestor` ca
  WHERE ca.ancestor_concept_id IN (
    9201, 9203, 262, 42898160, 9202, 581476, 722455, 581458, 32036, 581478, 38004193
  )
),

LatestSurvey AS (
  SELECT
    answer.person_id,
    MAX(DATE(answer.survey_datetime)) AS latest_survey_date
  FROM `ds_survey` answer   
  WHERE question_concept_id IN (
      SELECT DISTINCT concept_id                         
      FROM `cb_criteria` c                         
      JOIN (SELECT CAST(cr.id AS STRING) AS id                               
            FROM `cb_criteria` cr                               
            WHERE concept_id IN (1585855) AND domain_id = 'SURVEY') a 
      ON c.path LIKE CONCAT('%', a.id, '.%')                         
      WHERE domain_id = 'SURVEY' AND type = 'PPI' AND subtype = 'QUESTION'
  )
  GROUP BY answer.person_id
),

VisitClassification AS (
  SELECT 
    vo.person_id, 
    vo.visit_occurrence_id, 
    vo.visit_concept_id, 
    DATE(vo.visit_start_date) AS visit_start_date,
    COALESCE(dc.visit_type, 'Other') AS visit_type
  FROM `visit_occurrence` vo
  LEFT JOIN DescendantConcepts dc ON vo.visit_concept_id = dc.descendant_concept_id
),

UniqueDailyVisits AS (
  SELECT DISTINCT person_id, visit_type, visit_start_date
  FROM VisitClassification
),

PreSurveyVisits AS (
  SELECT 
    u.person_id, 
    u.visit_type, 
    u.visit_start_date,
    s.latest_survey_date
  FROM UniqueDailyVisits u
  INNER JOIN LatestSurvey s ON u.person_id = s.person_id  -- Restrict to only those with survey data
  WHERE u.visit_start_date >= DATE('2018-01-01') AND u.visit_start_date < s.latest_survey_date
),

IndividualVisitCounts AS (
  SELECT 
    person_id, 
    visit_type, 
    COUNT(DISTINCT visit_start_date) AS visit_count
  FROM PreSurveyVisits
  GROUP BY person_id, visit_type
),

IndividualVisitsPerMonth AS (
  SELECT 
    s.person_id, 
    ivc.visit_type, 
    ROUND(COALESCE(SUM(ivc.visit_count), 0) / GREATEST(DATE_DIFF(s.latest_survey_date, DATE('2018-01-01'), MONTH), 1), 2) AS visits_per_month
  FROM LatestSurvey s
  INNER JOIN IndividualVisitCounts ivc ON s.person_id = ivc.person_id
  GROUP BY s.person_id, ivc.visit_type, s.latest_survey_date
),

CombinedVisits AS (
  SELECT
    person_id,
    visit_start_date,
    CASE
      WHEN visit_type IN ('Inpatient_Visit', 'ER_Visit', 'ER_Inpatient_Visit') THEN 'Combined_Inpatient_ER'
      WHEN visit_type IN ('Outpatient_Visit', 'Telehealth_Visit') THEN 'Combined_Outpatient_Telehealth'
    END AS combined_visit_type
  FROM PreSurveyVisits
),

CombinedVisitCounts AS (
  SELECT
    person_id,
    combined_visit_type,
    COUNT(DISTINCT visit_start_date) AS visit_count 
  FROM CombinedVisits
  GROUP BY person_id, combined_visit_type
),

CombinedVisitsPerMonth AS (
  SELECT
    s.person_id,
    cvc.combined_visit_type,
    ROUND(COALESCE(SUM(cvc.visit_count), 0) / GREATEST(DATE_DIFF(s.latest_survey_date, DATE('2018-01-01'), MONTH), 1), 2) AS visits_per_month
  FROM LatestSurvey s
  INNER JOIN CombinedVisitCounts cvc ON s.person_id = cvc.person_id
  GROUP BY s.person_id, cvc.combined_visit_type, s.latest_survey_date
),

AllVisitsCounts AS (
  SELECT
    person_id,
    COUNT(DISTINCT visit_start_date) AS visit_count
  FROM PreSurveyVisits
  GROUP BY person_id
),

AllVisitsPerMonth AS (
  SELECT
    s.person_id,
    ROUND(COALESCE(SUM(avc.visit_count), 0) / GREATEST(DATE_DIFF(s.latest_survey_date, DATE('2018-01-01'), MONTH), 1), 2) AS All_Visits
  FROM LatestSurvey s
  INNER JOIN AllVisitsCounts avc ON s.person_id = avc.person_id
  GROUP BY s.person_id, s.latest_survey_date
),

PivotedVisits AS (
  SELECT 
    ivpm.person_id,
    MAX(CASE WHEN ivpm.visit_type = 'Outpatient_Visit' THEN ivpm.visits_per_month ELSE 0 END) AS Outpatient_Visit,
    MAX(CASE WHEN ivpm.visit_type = 'Inpatient_Visit' THEN ivpm.visits_per_month ELSE 0 END) AS Inpatient_Visit,
    MAX(CASE WHEN ivpm.visit_type = 'ER_Visit' THEN ivpm.visits_per_month ELSE 0 END) AS ER_Visit,
    MAX(CASE WHEN cvpm.combined_visit_type = 'Combined_Inpatient_ER' THEN cvpm.visits_per_month ELSE 0 END) AS Combined_Inpatient_ER,
    MAX(CASE WHEN cvpm.combined_visit_type = 'Combined_Outpatient_Telehealth' THEN cvpm.visits_per_month ELSE 0 END) AS Combined_Outpatient_Telehealth,
    avpm.All_Visits
  FROM IndividualVisitsPerMonth ivpm
  LEFT JOIN CombinedVisitsPerMonth cvpm ON ivpm.person_id = cvpm.person_id
  LEFT JOIN AllVisitsPerMonth avpm ON ivpm.person_id = avpm.person_id
  GROUP BY ivpm.person_id, avpm.All_Visits
)

SELECT * FROM PivotedVisits
ORDER BY person_id", sep="")

visit_data <- retrieve_data(visit_observation_sql)

# Grab all earliest conditions of interest 

# Read in comorbidities of interest from local spreadseheet
comorbidity_concept_sets <- read_csv("comorbidity_concept_sets.csv")

# Define function to batch process retrieval of condition_occurrence data
retrieve_condition_occurrence_batch <- function(concept_ids) {
  concept_id_str <- paste(concept_ids, collapse = ", ")
  
  # SQL query to retrieve condition_occurrence rows for the given batch of concept_ids
  condition_sql <- paste("
    SELECT
      c.person_id,
      c.condition_concept_id,
      c.condition_start_date
    FROM
      condition_occurrence c
    WHERE
      c.condition_concept_id IN (", concept_id_str, ")
  ", sep = "")
  
  # Execute the query and return the result
  bq_table_download(
    bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), condition_sql, billing = Sys.getenv("GOOGLE_PROJECT"))
  )
}

# Split the concept IDs into manageable batches (e.g., 5000 per batch)
batch_size <- 5000
concept_ids_list <- split(comorbidity_concept_sets$Concept_ID, ceiling(seq_along(comorbidity_concept_sets$Concept_ID) / batch_size))

# Retrieve all condition_occurrence rows for each batch and combine them into one dataframe
condition_occurrence_data <- map_dfr(concept_ids_list, retrieve_condition_occurrence_batch)

rm(concept_ids_list)

## 2 Dataset Post-Processing

# Process the basics survey

# Define question mapping
basics_question_mapping <- tibble(
  Q_label = paste0("Q_", 1:9),
  question = c(
    "The Basics: Birthplace",
    "Education Level: Highest Grade",
    "Marital Status: Current Marital Status",
    "Insurance: Health Insurance",
    "Health Insurance: Health Insurance Type",
    "Health Insurance: Insurance Type Update",
    "Employment: Employment Status",
    "Income: Annual Income",
    "Home Own: Current Home Own"
  )
)

# Join question mapping and clean data
basics_data <- basics_data %>%
  left_join(basics_question_mapping, by = "question") %>%  # Map questions to Q_n labels
  filter(!is.na(Q_label)) %>%  # Remove unmatched questions
  mutate(
    survey_datetime = as.Date(survey_datetime)  # Ensure survey_datetime is in Date format
  )

# Pivot to wide format while keeping survey_datetime
basics_data <- basics_data %>%
  select(person_id, survey_datetime, Q_label, question_concept_id, answer) %>%
  pivot_wider(
    id_cols = c(person_id, survey_datetime),   # Keep survey_datetime so we can sort by it later
    names_from = Q_label,                      # Each Q_n label becomes a set of columns
    values_from = answer,                       # Only pivot answers
    names_glue = "{.value}_{Q_label}",         # Create columns like answer_Q_1, answer_Q_2, etc.
    values_fn = list(answer = ~ first(.))      # Take the first value if there are duplicates
  )

# Ensure correct data types
basics_data <- basics_data %>%
  mutate(
    survey_datetime = as.Date(survey_datetime),  # Convert back to Date format if needed
    across(starts_with("answer_"), as.character) # Ensure answer columns are character
  )

# Grab the latest survey data per person
basics_data <- basics_data %>%
  arrange(person_id, desc(survey_datetime)) %>%  # Sort by survey date (latest first)
  group_by(person_id) %>%
  slice(1) %>%  # Select the latest survey response for each person
  ungroup() %>%
  mutate(latest_survey_date = survey_datetime) %>%  # Add a column for latest survey date
  select(person_id, latest_survey_date, everything())  # Reorder columns

rm(basics_question_mapping)

# Clean up output of basics data
basics_data <- basics_data %>% 
  mutate(
    birthplace = case_when(
      answer_Q_1 == 'Birthplace: USA' ~ "USA",
      answer_Q_1 == 'PMI: Other' ~ "Other",
      TRUE ~ 'Missing/Unknown'
    ),
    education_level = case_when(
      answer_Q_2 == 'Highest Grade: Advanced Degree' ~ "Advanced Degree",
      answer_Q_2 == 'Highest Grade: College Graduate' ~ "College Graduate",
      answer_Q_2 == 'Highest Grade: College One to Three' ~ "College One to Three",
      answer_Q_2 == 'Highest Grade: Twelve Or GED' ~ "Twelve Or GED",
      answer_Q_2 == 'Highest Grade: Nine Through Eleven' ~ "Nine Through Eleven",
      answer_Q_2 %in% c('Highest Grade: Five Through Eight', 
                        'Highest Grade: One Through Four',
                        'Highest Grade: Never Attended'
      ) ~ "<Nine",
      TRUE ~ "Missing/Unknown"
    ),
    marital_status = case_when(
      answer_Q_3 == 'Current Marital Status: Married' ~ "Married",
      answer_Q_3 == 'Current Marital Status: Never Married' ~ "Never Married",
      answer_Q_3 == 'Current Marital Status: Divorced' ~ "Divorced",
      answer_Q_3 %in% c('Current Marital Status: Living With Partner', 
                        'Current Marital Status: Separated',
                        'Current Marital Status: Widowed'
      ) ~ "Other",
      TRUE ~ "Missing/Unknown"
    ),
    health_insurance = case_when(
      answer_Q_4 == 'Health Insurance: Yes' ~ "Yes",
      answer_Q_4 == 'Health Insurance: No' ~ "No",
      TRUE ~ "Missing/Unknown"
    ),
    employment_status = case_when(
      answer_Q_7 %in% c('Employment Status: Employed For Wages',
                        'Employment Status: Self Employed') ~ "Employed",
      answer_Q_7 == 'Employment Status: Retired' ~ "Retired",
      answer_Q_7 == 'Employment Status: Unable To Work' ~ "Unable To Work",
      answer_Q_7 %in% c('Employment Status: Out Of Work One Or More',
                        'Employment Status: Out Of Work Less Than One') ~ "Out of Work",
      answer_Q_7 == 'Employment Status: Student' ~ "Student",
      answer_Q_7 == 'Employment Status: Homemaker' ~ "Homemaker",
      TRUE ~ "Missing/Unknown"
    ),
    annual_household_income = case_when(
      answer_Q_8 %in% c('Annual Income: less 10k',
                        'Annual Income: 10k 25k') ~ "Less than $25k",
      answer_Q_8 %in% c('Annual Income: 25k 35k',
                        'Annual Income: 35k 50k') ~ "$25k-$50k",
      answer_Q_8 == 'Annual Income: 50k 75k' ~ "$50k-$75k",
      answer_Q_8 == 'Annual Income: 75k 100k' ~ "$75k-$100k",
      answer_Q_8 %in% c('Annual Income: 100k 150k',
                        'Annual Income: 150k 200k') ~ "$100k-$200k",
      answer_Q_8 == 'Annual Income: more 200k' ~ "More than $200k",
      TRUE ~ "Missing/Unknown"
    ),
    current_home_own = case_when(
      answer_Q_9 == 'Current Home Own: Own' ~ "Own",
      answer_Q_9 == 'Current Home Own: Rent' ~ "Rent",
      answer_Q_9 == 'Current Home Own: Other Arrangement' ~ "Other Arrangement",
      TRUE ~ "Missing/Unknown"
    )
  )

basics_data <- subset(basics_data, select = c('person_id', 'birthplace', 
                                              "education_level", "marital_status", "health_insurance", "employment_status",
                                              "annual_household_income", "current_home_own"))

# Map survey questions to standardized labels
lifestyle_data <- lifestyle_data %>%
  left_join(
    tibble(
      Q_label = paste0("Q_", 1:31),
      question_text = c(
        "Smoking: 100 Cigs Lifetime", "Smoking: Smoke Frequency", "Smoking: Daily Smoke Starting Age",
        "Smoking: Number Of Years", "Smoking: Serious Quit Attempt", "Attempt Quit Smoking: Completely Quit Age",
        "Smoking: Current Daily Cigarette Number", "Smoking: Average Daily Cigarette Number", 
        "Electronic Smoking: Electric Smoke Participant", "Electronic Smoking: Electric Smoke Frequency",
        "Cigar Smoking: Cigar Smoke Participant", "Cigar Smoking: Current Cigar Frequency",
        "Hookah Smoking: Hookah Smoke Participant", "Hookah Smoking: Current Hookah Frequency",
        "Smokeless Tobacco: Smokeless Tobacco Participant", "Smokeless Tobacco: Smokeless Tobacco Frequency",
        "Alcohol: Alcohol Participant", "Alcohol: Drink Frequency Past Year", "Alcohol: Average Daily Drink Count",
        "Alcohol: 6 or More Drinks Occurrence", "Recreational Drug Use: Which Drugs Used",
        "Past 3 Month Use Frequency: Marijuana 3 Month Use", "Past 3 Month Use Frequency: Cocaine 3 Month Use",
        "Past 3 Month Use Frequency: Prescription Stimulant 3 Month Use", "Past 3 Month Use Frequency: Other Stimulant 3 Month Use",
        "Past 3 Month Use Frequency: Inhalant 3 Month Use", "Past 3 Month Use Frequency: Sedative 3 Month Use",
        "Past 3 Month Use Frequency: Hallucinogen 3 Month Use", "Past 3 Month Use Frequency: Street Opioid 3 Month Use",
        "Past 3 Month Use Frequency: Prescription Opioid 3 Month Use", "Past 3 Month Use Frequency: Other 3 Month Use"
      )
    ),
    by = c("question" = "question_text")
  ) %>%
  filter(!is.na(Q_label))  # Remove unmatched questions

# Reshape the dataset to a wide format
lifestyle_data <- lifestyle_data %>%
  select(person_id, survey_datetime, Q_label, answer) %>%
  pivot_wider(
    id_cols = c(person_id, survey_datetime),  
    names_from = Q_label,  
    values_from = answer,  
    names_glue = "answer_{Q_label}",  
    values_fn = list(answer = ~ first(.))  
  )

# Ensure required columns exist before processing
if (!all(c("answer_Q_1", "answer_Q_2", "answer_Q_5") %in% colnames(lifestyle_data))) {
  stop("Error: Expected smoking-related columns (answer_Q_1, answer_Q_2, answer_Q_5) are missing after pivoting.")
}

# Standardize response values for relevant smoking-related questions
lifestyle_data <- lifestyle_data %>%
  mutate(
    original_answer_Q_1 = answer_Q_1,  # Keep original values for debugging
    original_answer_Q_2 = answer_Q_2,
    original_answer_Q_5 = answer_Q_5,
    
    # Extract standardized responses
    answer_Q_1 = str_extract(answer_Q_1, "(Yes|No|Don't Know|Prefer Not To Answer)$"),
    answer_Q_2 = str_extract(answer_Q_2, "(Every Day|Some Days|Not At All|Don't Know|Prefer Not To Answer)$"),
    answer_Q_5 = str_extract(answer_Q_5, "(Attempt Quit Smoking|No Attempt Quit Smoking|Don't Know|Prefer Not To Answer)$")
  )

# Select the most recent survey response, prioritizing those with a valid 100 Cigs Lifetime answer
lifestyle_data <- lifestyle_data %>%
  arrange(person_id, desc(survey_datetime)) %>%
  group_by(person_id) %>%
  mutate(has_valid_100_cigs = !is.na(answer_Q_1) & answer_Q_1 %in% c("Yes", "No")) %>%
  filter(if (any(has_valid_100_cigs)) has_valid_100_cigs else row_number() == 1) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(latest_survey_date = survey_datetime) %>%
  select(person_id, latest_survey_date, everything())

# Create derived smoking status indicators
lifestyle_data <- lifestyle_data %>%
  mutate(
    smoking_100_cigs = case_when(
      answer_Q_1 == "Yes" ~ 1,
      answer_Q_1 == "No" ~ 0,
      TRUE ~ NA_real_
    ),
    
    smoking_frequency = answer_Q_2,
    smoking_start_age = suppressWarnings(as.numeric(answer_Q_3)),
    smoking_years = suppressWarnings(as.numeric(answer_Q_4)),
    smoking_quit_attempt = case_when(
      answer_Q_5 == "Attempt Quit Smoking" ~ 1,
      answer_Q_5 == "No Attempt Quit Smoking" ~ 0,
      TRUE ~ NA_real_
    ),
    smoking_quit_age = suppressWarnings(as.numeric(answer_Q_6)),
    smoking_current_cigarettes_per_day = suppressWarnings(as.numeric(answer_Q_7)),
    smoking_avg_daily_cigarettes = suppressWarnings(as.numeric(answer_Q_8)),
    
    # Define mutually exclusive current and former smoker indicators
    is_current_smoker = ifelse(
      smoking_100_cigs == 1 & smoking_frequency %in% c("Every Day", "Some Days") & is.na(smoking_quit_age),
      1, 0
    ),
    
    is_former_smoker = ifelse(
      smoking_100_cigs == 1 & (smoking_frequency == "Not At All" | !is.na(smoking_quit_age)),
      1, 0
    ),
    
    # Assign categorical smoking status
    smoking_status = case_when(
      is_current_smoker == 1 ~ "Current Smoker",
      is_former_smoker == 1 ~ "Former Smoker",
      smoking_100_cigs == 1 & (smoking_frequency %in% c("Don't Know", "Prefer Not To Answer") | is.na(smoking_frequency)) ~ "Current or Former Smoker",
      smoking_100_cigs == 0 ~ "Non-Smoker",
      TRUE ~ "Unknown"
    )
  ) %>%
  
  # Retain original responses for debugging and ensure relevant columns are included
  select(person_id, latest_survey_date, 
         original_answer_Q_1, original_answer_Q_2, original_answer_Q_5,  # Original responses
         smoking_100_cigs, smoking_frequency, smoking_start_age, smoking_years, smoking_quit_attempt, smoking_quit_age,
         smoking_current_cigarettes_per_day, smoking_avg_daily_cigarettes,
         is_current_smoker, is_former_smoker, smoking_status)

# Identify the earliest conditions from a pre-specified dataset of concept sets

# Group by person_id and condition_concept_id to get the earliest condition_start_date
earliest_conditions <- condition_occurrence_data %>%
  group_by(person_id, condition_concept_id) %>%
  summarise(earliest_condition_date = min(condition_start_date), .groups = "drop")

# Merge with comorbidity_concept_sets to map concept IDs to Indicator_Prefix and pivot

# Join with comorbidity_concept_sets to get Indicator_Prefix for each concept_id
earliest_conditions_with_prefix <- earliest_conditions %>%
  left_join(comorbidity_concept_sets, by = c("condition_concept_id" = "Concept_ID"))

# Pivot the data so each Indicator_Prefix becomes a column, containing the earliest date for that prefix
earliest_conditions_wide <- earliest_conditions_with_prefix %>%
  group_by(person_id, Indicator_Prefix) %>%
  summarise(earliest_condition_date = min(earliest_condition_date), .groups = "drop") %>%
  pivot_wider(
    names_from = Indicator_Prefix,
    values_from = earliest_condition_date,
    names_prefix = "earliest_",
    values_fill = NA
  )

# Merge survey datasets with EHR-linked data using a full join to keep all persons
combined_data <- basics_data %>%
  full_join(lifestyle_data, by = "person_id") %>%
  full_join(EHR_linked_data, by = "person_id")

# Merge datasets while keeping all persons in combined_data
combined_data <- combined_data %>%
  left_join(earliest_conditions_wide, by = "person_id")

# Identify all condition columns (those starting with "earliest_")
condition_cols <- grep("^earliest_", colnames(combined_data), value = TRUE)

# Convert all condition columns to Date format (if not already)
combined_data <- combined_data %>%
  mutate(across(all_of(condition_cols), as.Date)) 

# Create binary indicators (1 if condition date is before latest survey date, else 0)
combined_data <- combined_data %>%
  mutate(across(all_of(condition_cols), 
                ~ ifelse(!is.na(.) & . < latest_survey_date, 1, 0), 
                .names = "{str_remove(.col, 'earliest_')}"))

# Drop original earliest_* columns after indicators are created
combined_data <- combined_data %>%
  select(-all_of(condition_cols))

# Create pre-survey tobacco observations from observation data

# Compute total tobacco observations before survey for each person
tobacco_count <- tobacco_data %>%
  inner_join(combined_data, by = "person_id") %>%
  filter(observation_date < latest_survey_date) %>%
  group_by(person_id) %>%
  summarise(num_tobacco_observations = n(), .groups = "drop")  # Count total observations

# Identify latest valid smoking status before survey
tobacco_latest <- tobacco_data %>%
  inner_join(combined_data, by = "person_id") %>%
  filter(observation_date < latest_survey_date) %>%
  group_by(person_id) %>%
  slice_max(observation_date, with_ties = FALSE) %>%  # Latest observation per person
  summarise(
    latest_tobacco_observation_date = max(observation_date, na.rm = TRUE),
    latest_smoker_status = case_when(
      max(current_smoker, na.rm = TRUE) == 1 ~ "Current Smoker",
      max(former_smoker, na.rm = TRUE) == 1 ~ "Former Smoker",
      max(non_smoker, na.rm = TRUE) == 1 ~ "Non-Smoker",
      TRUE ~ NA_character_
    ),
    .groups = "drop"
  )

combined_data <- combined_data %>%
  full_join(tobacco_latest, by = "person_id")

combined_data <- combined_data %>%
  full_join(tobacco_count, by = "person_id")

# Clean up temporary dataframes
rm(tobacco_count, tobacco_latest)

# Final clean up for demographic data

# Compute age at latest survey
combined_data <- combined_data %>%
  mutate(age = floor(interval(start = date_of_birth, end = latest_survey_date) / years(1))) 

# Create age group, clean race, clean ethnicity, and combined race/ethnicity variables
combined_data <- combined_data %>%
  mutate(
    # Age Group
    age_group = case_when(
      age < 18 ~ "<18",
      age < 45 ~ "18-44",
      age < 65 ~ "45-64",
      age >= 65 ~ "65+",
      TRUE ~ "Missing/Unknown"
    ),
    
    # Race Categories
    race = case_when(
      race_concept_id == 8516 ~ "Black or African American",
      race_concept_id == 2100000001 ~ "Hispanic, Latino, or Spanish",
      race_concept_id == 8527 ~ "White",
      race_concept_id == 8515 ~ "Asian",
      race_concept_id == 2000000008 ~ "Multiple",
      race_concept_id == 45882607 ~ "None of these",
      race_concept_id == 38003615 ~ "Middle Eastern or North African",
      race_concept_id == 1177221 ~ "I prefer not to answer",
      race_concept_id == 8557 ~ "Native Hawaiian or Other Pacific Islander",
      TRUE ~ NA_character_
    ),
    
    # Clean Race
    clean_race = case_when(
      race_concept_id == 8527 ~ "White",
      race_concept_id == 8516 ~ "Black or African American",
      race_concept_id == 2000000008 ~ "Multiple",  # Keep "Multiple" as a distinct category
      race_concept_id %in% c(8515, 8557, 2100000001, 45882607, 38003615) ~ "Other",
      is.na(race_concept_id) ~ "Missing/Unknown",
      TRUE ~ "Missing/Unknown"
    ),
    
    # Ethnicity Categories
    ethnicity = case_when(
      ethnicity_concept_id == 903079 ~ "Prefer Not To Answer",
      ethnicity_concept_id == 903096 ~ "Skip",
      ethnicity_concept_id == 1586148 ~ "None of these fully describe me",
      ethnicity_concept_id == 38003563 ~ "Hispanic or Latino",
      ethnicity_concept_id == 38003564 ~ "Not Hispanic or Latino",
      TRUE ~ NA_character_
    ),
    
    # Clean Ethnicity
    clean_ethnicity = case_when(
      is.na(ethnicity_concept_id) ~ "Missing/Unknown",
      ethnicity_concept_id == 38003563 ~ "Hispanic or Latino",
      ethnicity_concept_id == 38003564 ~ "Not Hispanic or Latino",
      ethnicity_concept_id == 1586148 ~ "Other",
      TRUE ~ "Missing/Unknown"
    ),
    
    # Race and Ethnicity Combination
    race_and_ethnicity = case_when(
      clean_ethnicity == "Hispanic or Latino" ~ 'Hispanic/Latinx',
      clean_race == "White" & clean_ethnicity == "Not Hispanic or Latino" ~ 'Non-Hispanic/Latinx White',
      clean_race == "Black or African American" & clean_ethnicity == "Not Hispanic or Latino" ~ 'Non-Hispanic/Latinx Black or AA',
      clean_race == "Multiple" & clean_ethnicity == "Not Hispanic or Latino" ~ 'Non-Hispanic/Latinx Multiple',
      clean_race == "Other" & clean_ethnicity == "Not Hispanic or Latino" ~ 'Non-Hispanic/Latinx Other',
      clean_race == "Missing/Unknown" & clean_ethnicity == "Missing/Unknown" ~ "Missing/Unknown",
      TRUE ~ "Other/Unclassified"
    )
  )

# Add has_EHR_data indicator to combined_data
combined_data <- combined_data %>%
  mutate(has_EHR_data = if_else(person_id %in% EHR_linked_data$person_id, 1, 0))

# Cleanup
rm(basics_data, condition_occurrence_data, earliest_conditions_wide, 
   earliest_conditions, earliest_conditions_with_prefix, EHR_linked_data, 
   lifestyle_data, lifestyle_smoking_summary, tobacco_data, tobacco_summary, 
   visit_data, tbl_test, table_p1, tbl1, problematic_cases, 
   merged_models_current_smoker, uv_model, test_combined)

# Create analytic dataset

# List of columns to drop
columns_to_remove <- c(
  "person_id", "birthplace", 
  "original_answer_Q_1", "original_answer_Q_2", "original_answer_Q_5", 
  "smoking_100_cigs", "gender_concept_id", 
  "gender", "date_of_birth", "race_concept_id", 
  "race", "ethnicity_concept_id", "ethnicity", 
  "sex_at_birth_concept_id", "clean_race", "clean_ethnicity", 
  "latest_tobacco_observation_date"
)

# Remove columns and rename dataframe
analytic_data <- combined_data %>%
  select(-all_of(columns_to_remove))

# Filter down to final cohort

# Filter out patients with has_EHR_data == 0
analytic_data <- analytic_data %>%
  filter(has_EHR_data == 1)

# Filter out patients with missing age or age not in the 18-99 range
analytic_data <- analytic_data %>%
  filter(!is.na(age) & age >= 18 & age <= 99)

# Filter out patients with smoking_status == 'Unknown'
analytic_data <- analytic_data %>%
  filter(smoking_status != "Unknown")

# Filter out patients with sex_at_birth not in 'Female' or 'Male'
analytic_data <- analytic_data %>%
  filter(sex_at_birth %in% c("Female", "Male"))

# Create EHR-based smoking indicators
analytic_data <- analytic_data %>%
  mutate(
    EHR_based_smoker = if_else(replace_na(latest_smoker_status, "") == "Current Smoker" | replace_na(NICOTINE, 0) == 1, 1, 0),
    EHR_based_current_former_smoker = if_else(replace_na(latest_smoker_status, "") %in% c("Current Smoker", "Former Smoker") | replace_na(NICOTINE, 0) == 1, 1, 0)
  )

# Create survey-based smoking indicators
analytic_data <- analytic_data %>%
  mutate(
    survey_based_smoker = if_else(smoking_status == "Current Smoker", 1, 0),
    survey_based_current_former_smoker = if_else(smoking_status %in% c("Current Smoker", "Former Smoker", "Current or Former Smoker"), 1, 0)
  )

# Create agreement variables
# Classify agreement and disagreement for current smoker and current/former smoker
analytic_data <- analytic_data %>%
  mutate(
    # Agreement categories for current smoker status
    current_smoker_agreement = case_when(
      survey_based_smoker == 1 & EHR_based_smoker == 1 ~ "Both Yes",   # True agreement
      survey_based_smoker == 0 & EHR_based_smoker == 0 ~ "Both No",    # True agreement
      survey_based_smoker == 1 & EHR_based_smoker == 0 ~ "Survey-Only", # Disagreement
      survey_based_smoker == 0 & EHR_based_smoker == 1 ~ "EHR-Only"    # Disagreement
    ),
    
    # Binary indicator for current smoker agreement (1 = agreement, 0 = disagreement)
    current_smoker_agreement_binary = if_else(
      current_smoker_agreement %in% c("Both Yes", "Both No"), 1, 0
    ),
    
    # Agreement categories for current or former smoker status
    current_former_smoker_agreement = case_when(
      survey_based_current_former_smoker == 1 & EHR_based_current_former_smoker == 1 ~ "Both Yes",   # True agreement
      survey_based_current_former_smoker == 0 & EHR_based_current_former_smoker == 0 ~ "Both No",    # True agreement
      survey_based_current_former_smoker == 1 & EHR_based_current_former_smoker == 0 ~ "Survey-Only", # Disagreement
      survey_based_current_former_smoker == 0 & EHR_based_current_former_smoker == 1 ~ "EHR-Only"    # Disagreement
    ),
    
    # Binary indicator for current or former smoker agreement (1 = agreement, 0 = disagreement)
    current_former_smoker_agreement_binary = if_else(
      current_former_smoker_agreement %in% c("Both Yes", "Both No"), 1, 0
    )
  )


# Set factor levels
analytic_data <- analytic_data %>%
  mutate(
    # Age as a numeric variable
    age = as.numeric(age),
    
    # Age group as an ordered factor
    age_group = factor(age_group, levels = c("18-44", "45-64", "65+")),
    
    # Combined race and ethnicity variable
    race_and_ethnicity = factor(race_and_ethnicity, levels = c(
      "Hispanic/Latinx", "Non-Hispanic/Latinx White", "Non-Hispanic/Latinx Black or AA",
      "Non-Hispanic/Latinx Multiple", "Non-Hispanic/Latinx Other", "Other/Unclassified",
      "Missing/Unknown"
    )),
    
    # Smoking status categories 
    smoking_status = factor(smoking_status,
                            levels = c("Current Smoker", "Current or Former Smoker", "Former Smoker", "Non-Smoker")
    ),
    
    # Smoking frequency categories
    smoking_frequency = factor(
      replace_na(smoking_frequency, "Unknown"),
      levels = c("Every Day", "Some Days", "Not At All", "Prefer Not To Answer", "Unknown")
    ), 
    
    # Education level 
    education_level = factor(education_level, levels = c(
      "<Nine", "Nine Through Eleven", "Twelve Or GED",
      "College One to Three", "College Graduate", "Advanced Degree",
      "Missing/Unknown"
    )),
    
    # Marital status 
    marital_status = factor(marital_status, levels = c(
      "Never Married", "Married", "Divorced", "Other", "Missing/Unknown"
    )),
    
    # Health insurance 
    health_insurance = factor(health_insurance, levels = c(
      "No", "Yes", "Missing/Unknown"
    )),
    
    # Employment status
    employment_status = factor(employment_status, levels = c(
      "Employed", "Homemaker", "Student", "Out of Work", 
      "Retired", "Unable To Work", "Missing/Unknown"
    )),
    
    # Annual household income
    annual_household_income = factor(annual_household_income, levels = c(
      "Less than $25k", "$25k-$50k", "$50k-$75k", "$75k-$100k", 
      "$100k-$200k", "More than $200k", "Missing/Unknown"
    )),
    
    # Home ownership categories
    current_home_own = factor(current_home_own, levels = c(
      "Own", "Rent", "Other Arrangement", "Missing/Unknown"
    ))
  )

# Overall summary statistics by smoking status
table1 <- analytic_data %>% 
  tbl_summary(by=smoking_status)
# add_overall() %>% 
# add_p()

table1

### 1. Cohen's Kappa Analysis

# Function to compute and format Cohen's Kappa results
compute_kappa <- function(data, survey_var, ehr_var, measure_name) {
  kappa_data <- data %>%
    select(!!sym(survey_var), !!sym(ehr_var))
  
  kappa_result <- kappa2(kappa_data)
  
  # Compute standard error and confidence intervals
  se <- kappa_result$value / kappa_result$statistic
  ci_lower <- round(kappa_result$value - 1.96 * se, 3)
  ci_upper <- round(kappa_result$value + 1.96 * se, 3)
  
  # Return a formatted tibble with labeled results
  tibble(
    Measure = measure_name,
    `Cohen's Kappa` = round(kappa_result$value, 3),
    `95% CI Lower` = ci_lower,
    `95% CI Upper` = ci_upper
  )
}

# Compute Cohen's Kappa for Current Smoker Agreement
kappa_current_smoker <- compute_kappa(
  analytic_data, "survey_based_smoker", "EHR_based_smoker", "Current Smoker Agreement"
)

# Compute Cohen's Kappa for Current or Former Smoker Agreement
kappa_current_former_smoker <- compute_kappa(
  analytic_data, "survey_based_current_former_smoker", "EHR_based_current_former_smoker",
  "Current or Former Smoker Agreement"
)

# Combine results into a single table
kappa_table <- bind_rows(kappa_current_smoker, kappa_current_former_smoker)

# Display Cohen's Kappa table
kappa_table


### 2. Sensitivity and Specificity Analysis

# Function to compute diagnostic performance metrics
calculate_metrics <- function(confusion_matrix, measure_name) {
  # Extract values from 2x2 confusion matrix
  TP <- confusion_matrix["1", "1"]  # True Positives (Survey+ and EHR+)
  TN <- confusion_matrix["0", "0"]  # True Negatives (Survey- and EHR-)
  FP <- confusion_matrix["0", "1"]  # False Positives (Survey+ but EHR-)
  FN <- confusion_matrix["1", "0"]  # False Negatives (Survey- but EHR+)
  
  # Calculate metrics with protection against division by zero
  sensitivity <- ifelse((TP + FN) > 0, TP / (TP + FN), NA)
  specificity <- ifelse((TN + FP) > 0, TN / (TN + FP), NA)
  ppv <- ifelse((TP + FP) > 0, TP / (TP + FP), NA)
  npv <- ifelse((TN + FN) > 0, TN / (TN + FN), NA)
  
  # Return results as formatted tibble
  tibble(
    Measure = measure_name,
    Sensitivity = round(sensitivity, 3),
    Specificity = round(specificity, 3),
    PPV = round(ppv, 3),
    NPV = round(npv, 3)
  )
}

# Compute confusion matrix and performance metrics for Current Smoker Agreement
conf_matrix_current_smoker <- table(
  EHR = analytic_data$EHR_based_smoker,
  Survey = analytic_data$survey_based_smoker
)
performance_current_smoker <- calculate_metrics(
  conf_matrix_current_smoker, "Current Smoker Agreement"
)

# Compute confusion matrix and performance metrics for Current or Former Smoker Agreement
conf_matrix_current_former_smoker <- table(
  EHR = analytic_data$EHR_based_current_former_smoker,
  Survey = analytic_data$survey_based_current_former_smoker
)
performance_current_former_smoker <- calculate_metrics(
  conf_matrix_current_former_smoker, "Current or Former Smoker Agreement"
)

# Combine performance tables into a single table
performance_table <- bind_rows(performance_current_smoker, performance_current_former_smoker)

# Display Sensitivity & Specificity table
performance_table

# Update reference factor levels
# Age Group
analytic_data <- analytic_data %>%
  mutate(age_group = relevel(factor(age_group), ref = "45-64"))

# Race/Ethnicity
analytic_data <- analytic_data %>%
  mutate(race_and_ethnicity = relevel(factor(race_and_ethnicity), ref = "Non-Hispanic/Latinx White"))

# Education
analytic_data <- analytic_data %>%
  mutate(education_level = relevel(factor(education_level), ref = "Twelve Or GED"))

# Marital Status
analytic_data <- analytic_data %>%
  mutate(marital_status = relevel(factor(marital_status), ref = "Married"))

# Health Insurance
analytic_data <- analytic_data %>%
  mutate(health_insurance = relevel(factor(health_insurance), ref = "Yes"))

# Household Income
analytic_data <- analytic_data %>%
  mutate(annual_household_income = relevel(factor(annual_household_income), ref = "$75k-$100k"))


# Logistic Regression for current smoker alignment

# Create binary discrepancy variable
regression_data <- analytic_data %>%
  select(current_smoker_agreement_binary, age_group, race_and_ethnicity,
         education_level, marital_status, health_insurance,
         employment_status, annual_household_income, current_home_own)

# Univariable logistic regression
uv_model <- tbl_uvregression(
  data = regression_data, # note: you’ll need to subset to only the variables you want included
  method = glm,
  y = current_smoker_agreement_binary,
  method.args = list(family = binomial),
  exponentiate = TRUE
)

# Multivariable logistic regression
mv_model <- glm(
  current_smoker_agreement_binary ~ age_group + race_and_ethnicity + education_level +
    marital_status + health_insurance + employment_status + annual_household_income +
    current_home_own,
  data = regression_data,
  family = binomial
)

mv_table <- tbl_regression(
  mv_model,
  exponentiate = TRUE
) %>%
  add_n(location = "level") %>% # Add sample size at each level
  add_nevent(location = "level") %>% # Add number of events
  modify_table_body(
    ~ .x %>%
      dplyr::mutate(
        stat_nevent_rate =
          ifelse(
            !is.na(stat_nevent),
            paste0(style_sigfig(stat_nevent / stat_n, scale = 100), "%"),
            NA
          ),
        .after = stat_nevent
      )
  ) %>%
  modify_column_merge(
    pattern = "{stat_nevent} / {stat_n} ({stat_nevent_rate})",
    rows = !is.na(stat_nevent)
  ) %>%
  modify_header(stat_nevent = "**Event Rate**")

# Combine univariable and multivariable models
merged_models_current_smoker <- tbl_merge(
  tbls = list(uv_model, mv_table),
  tab_spanner = c("Univariable Model", "Multivariable Model")
)

merged_models_current_smoker


# Logistic Regression for current or former smoker alignment

# Create binary discrepancy variable
regression_data <- analytic_data %>%
  select(current_former_smoker_agreement_binary, age_group, race_and_ethnicity,
         education_level, marital_status, health_insurance,
         employment_status, annual_household_income, current_home_own)

# Univariable logistic regression
uv_model <- tbl_uvregression(
  data = regression_data, # note: you’ll need to subset to only the variables you want included
  method = glm,
  y = current_former_smoker_agreement_binary,
  method.args = list(family = binomial),
  exponentiate = TRUE
)

# Multivariable logistic regression
mv_model <- glm(
  current_former_smoker_agreement_binary ~ age_group + race_and_ethnicity + education_level +
    marital_status + health_insurance + employment_status + annual_household_income +
    current_home_own,
  data = regression_data,
  family = binomial
)

mv_table <- tbl_regression(
  mv_model,
  exponentiate = TRUE
) %>%
  add_n(location = "level") %>% # Add sample size at each level
  add_nevent(location = "level") %>% # Add number of events
  modify_table_body(
    ~ .x %>%
      dplyr::mutate(
        stat_nevent_rate =
          ifelse(
            !is.na(stat_nevent),
            paste0(style_sigfig(stat_nevent / stat_n, scale = 100), "%"),
            NA
          ),
        .after = stat_nevent
      )
  ) %>%
  modify_column_merge(
    pattern = "{stat_nevent} / {stat_n} ({stat_nevent_rate})",
    rows = !is.na(stat_nevent)
  ) %>%
  modify_header(stat_nevent = "**Event Rate**")

# Combine univariable and multivariable models
merged_models_current_or_former_smoker <- tbl_merge(
  tbls = list(uv_model, mv_table),
  tab_spanner = c("Univariable Model", "Multivariable Model")
)

merged_models_current_or_former_smoker