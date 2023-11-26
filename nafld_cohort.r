# NAFLD codes
# Inclusion
nafld_icd9_inclusion <- c('571.5')
nafld_icd10_inclusion <- c('K75.8', 'K76.0')

# Exclusion
nafld_icd9_exclusion <- c('571.0', '571.1', '571.2', '571.3', '070', '571.6', '576.1', '275.0', '275.1', '277.6', '453.0', '571.4', '571.6', '303', '305.0', '291', '357.5', '425.5', '535.3', '980.1', '980.9')
nafld_icd10_exclusion <- c('K70', 'B16', 'B17', 'B18', 'B19', '83.0A', 'K83.0F', 'K74.3', 'K75.4', 'E83.1', 'E83.02', 'E88.01', 'E88.02', 'I82.0', 'K76.5', 'K73.2', 'K73.9', 'K74.4', 'K74.5', 'F10', 'E24.4', 'G62.1', 'I42.6', 'K29.2', 'G31.2', 'G72.1', 'K85.2', 'K86.0', 'T51.0', 'T51.9', 'Y57.3', 'X65', 'Z50.2', 'Z71.4', 'Z72.1')

# get concepts for conditions of NAFLD (inclusion)

condition_codes_nafld_inclusion_icd9_string <- toString(sprintf("'%s'",nafld_icd9_inclusion))
condition_codes_nafld_inclusion_icd10_string <- toString(sprintf("'%s'",nafld_icd10_inclusion))


condition_nafld_inclusion_sql <- str_glue("
                                     
SELECT 
    c.concept_name,
    c.concept_code,
    c.concept_id
FROM 
    `{DATASET}.concept` c
    JOIN `{DATASET}.condition_occurrence` co
        ON c.concept_id = co.condition_source_concept_id
WHERE
    (vocabulary_id='ICD9CM' AND concept_code IN (%s))
    OR (vocabulary_id='ICD10CM' AND concept_code IN (%s))

GROUP BY
    c.concept_name,
    c.concept_code,
    c.concept_id

")


query <- sprintf(condition_nafld_inclusion_sql, condition_codes_nafld_inclusion_icd9_string, condition_codes_nafld_inclusion_icd10_string)
condition_concepts_nafld_inclusion_df <- download_data(query)

# get concepts for conditions of NAFLD (exclusion)

condition_codes_nafld_exclusion_icd9_string <- toString(sprintf("'%s'",nafld_icd9_exclusion))
condition_codes_nafld_exclusion_icd10_string <- toString(sprintf("'%s'",nafld_icd10_exclusion))


condition_nafld_exclusion_sql <- str_glue("
                                     
SELECT 
    c.concept_name,
    c.concept_code,
    c.concept_id
FROM 
    `{DATASET}.concept` c
    JOIN `{DATASET}.condition_occurrence` co
        ON c.concept_id = co.condition_source_concept_id
WHERE
    (vocabulary_id='ICD9CM' AND concept_code IN (%s))
    OR (vocabulary_id='ICD10CM' AND concept_code IN (%s))

GROUP BY
    c.concept_name,
    c.concept_code,
    c.concept_id

")


query <- sprintf(condition_nafld_exclusion_sql, condition_codes_nafld_exclusion_icd9_string, condition_codes_nafld_exclusion_icd10_string)
condition_concepts_nafld_exclusion_df <- download_data(query)

# Create strings of concept IDs for inclusion and exclusion
condition_concepts_nafld_inclusion_string <- paste(condition_concepts_nafld_inclusion_df$concept_id, collapse = ", ")
condition_concepts_nafld_exclusion_string <- paste(condition_concepts_nafld_exclusion_df$concept_id, collapse = ", ")

# SQL query to define nafld_cohort
nafld_sql <- str_glue("
SELECT *
FROM `{DATASET}.person`
WHERE
    person_id IN (SELECT person_id
                  FROM `{DATASET}.condition_occurrence`
                  WHERE condition_source_concept_id IN (%s))
    AND person_id NOT IN (SELECT person_id
                          FROM `{DATASET}.condition_occurrence`
                          WHERE condition_source_concept_id IN (%s))
")

# Prepare and execute the query
query <- sprintf(nafld_sql, condition_concepts_nafld_inclusion_string, condition_concepts_nafld_exclusion_string)
nafld_cohort <- download_data(query)
nafld_cohort
