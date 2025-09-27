-- Data Cleaning
-- Data taken from layoffs.csv by AlexTheAnalyst on github
SELECT *
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null Values or blank values
-- 4. Remove any Columns; instances where you should and shouldn't do this

CREATE TABLE layoffs_staging2
LIKE layoffs;

SELECT *
FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num2
FROM layoffs_staging2;


-- CHecking for duplicates
WITH duplicate_cte2 AS
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, location,
    industry, total_laid_off, percentage_laid_off, `date`,
    stage, country, funds_raised_millions) AS row_num2
	FROM layoffs_staging2
)
SELECT *
FROM duplicate_cte2
WHERE row_num2 > 1;

-- Confirming if they are duplicates
SELECT *
FROM layoffs_staging2
WHERE company = 'Casper';


WITH duplicate_cte2 AS
(
	SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, location,
    industry, total_laid_off, percentage_laid_off, `date`,
    stage, country, funds_raised_millions) AS row_num2
	FROM layoffs_staging2
)
DELETE
FROM duplicate_cte2
WHERE row_num2 > 1;


-- Inserted a copy of columns 
CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num2` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging3
WHERE row_num2 > 1;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) AS row_num2
FROM layoffs_staging2;

-- Deleting the row_num2 column
DELETE
FROM layoffs_staging3
WHERE row_num2 > 1;

-- Checking to see if the column was properly deleted
SELECT *
FROM layoffs_staging3;

-- Standardizing data
-- 

-- Trimming the company column 
SELECT company, (TRIM(company))
FROM layoffs_staging3;
-- Updating the column 
UPDATE layoffs_staging3
SET company = TRIM(company);
-- Checking if anything in the industry column needs trimming
SELECT DISTINCT industry
FROM layoffs_staging3
;
-- Updating the Crypto cells so that there are no duplicates (Crypto, CryptoCurrency, and Crypto Currency all become crypto)
UPDATE layoffs_staging3
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Removing the . from the end of United States
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging3
ORDER BY 1;
-- Updating the United States cell to not have the period
UPDATE layoffs_staging3
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- Changing date from text to years
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date` 
FROM layoffs_staging3;

-- Changes date from txt to date
ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;



SELECT * 
FROM layoffs_staging3;




-- Working with Null and Blank Values
-- 
-- Checking for null values
SELECT * 
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- Changing any cells that are empty to null
UPDATE layoffs_staging3
SET industry = NULL
WHERE industry = '';
-- Checking which cells are null or empty
SELECT *
FROM layoffs_staging3
WHERE industry IS NULL
OR industry = '';
-- Looking for Bally company
SELECT *
FROM layoffs_staging3
WHERE company LIKE 'Bally%';

-- Made sure there is no empty value for industry for Airbnb
SELECT *
FROM layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
-- Updating it
UPDATE layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Double checking everything is correctly updated
SELECT *
FROM layoffs_staging3;

-- Getting rid of null values
DELETE
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- Double checking everything is correctly deleted
SELECT *
FROM layoffs_staging3;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num2;

-- End of data cleaning
