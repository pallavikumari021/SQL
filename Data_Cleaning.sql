-- Data Cleaning
Select * from world_layoffs.layoffs;

-- 1.Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values.
-- 4. Remove Any Columns

CREATE Table layoff_staging 
like layoffs;

INSERT layoff_staging
Select *
From layoffs;
Select * from world_layoffs.layoff_staging;

-- Find Duplicates

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised_millions) AS row_num
FROM layoff_staging
)
SELECT *
FROM duplicates_cte;

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised_millions) AS row_num
FROM layoff_staging
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;



CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoff_staging2 where row_num > 1;

INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised_millions) AS row_num
FROM layoff_staging;

DELETE from layoff_staging2 where row_num > 1;

select * from layoff_staging2;
-- Standardizing data

SELECT company, TRIM(company)
FROM layoff_staging2;

UPDATE layoff_staging2
SET company = TRIM(company);

select DISTINCT industry
FROM layoff_staging2
order by 1;

Select *
from layoff_staging2
where industry like 'Crypto%';

Update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

Select *
From layoff_staging2
where country like 'United States%'
Order by 1;

Select distinct country, Trim(Trailing '.' from country)
from layoff_staging2
order by 1;

Update layoff_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
From layoff_staging2;

Update layoff_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


Alter Table layoff_staging2
Modify column `date` DATE;

Select * 
from layoff_staging2;

select * from 
layoff_staging2 where company ='Airbnb';

select t1.industry, t2.industry
from layoff_staging2 t1
Join layoff_staging2 t2
    ON t1.company = t2.company
where (t1.industry Is null Or t1.industry = '')
AND t2.industry IS NOT NULL;

Update layoff_staging2 t1
Join layoff_staging2 t2
    ON t1.company = t2.company
Set t1.industry = t2.industry
where  t1.industry IS NULL 
AND t2.industry IS NOT NULL;

select *
from layoff_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;


delete
from layoff_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
from layoff_staging2;

Alter table layoff_staging2
drop column row_num;