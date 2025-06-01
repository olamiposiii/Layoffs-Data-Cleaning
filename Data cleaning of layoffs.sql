-- Data cleaning project

select *
from layoffs;


-- 1. remove duplicate
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns

CREATE TABLE layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select*
from layoffs
;

-- indentifying duplicate

select *,
row_number() over(
partition by company, industry, total_laid_off,percentage_laid_off, 'date')as  row_num
from layoffs_staging 
;

with duplicate_cte as 
(
select *,
row_number() over(
partition by company,location, industry, total_laid_off,percentage_laid_off, 'date', stage, country, funds_raised, date_added)as  row_num
from layoffs_staging 
)

select *
from duplicate_cte
where row_num >1;


select * 
from layoffs_staging
where company = 'Microsoft';


with duplicate_cte as 
(
select *,
row_number() over(
partition by company,location, industry, total_laid_off,percentage_laid_off, 'date', stage, country, funds_raised, date_added)as  row_num
from layoffs_staging 
)

delete 
from duplicate_cte
where row_num >1;









CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text, 
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2 ;

insert into layoffs_staging2
select *,
row_number() over(
partition by company,location, industry, total_laid_off,percentage_laid_off, 'date', stage, country, funds_raised, date_added)as  row_num
from layoffs_staging ;


set sql_safe_updates = 0;


delete 
from layoffs_staging2 
where row_num > 1;

select *
from layoffs_staging2 ;


--- Standardizzing Data
-- trim the company name 

select company, trim(company)
from layoffs_staging2 
;

update layoffs_staging2
set company = trim(company);

select distinct(industry)
from layoffs_staging2 
order by 1;

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2
;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select *
from layoffs_staging2;


alter table layoffs_staging2
modify column `date` date;

-- null value 


select *
from layoffs_staging2
where total_laid_off is null
and 
percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';


delete
from layoffs_staging2
where total_laid_off =''
and
percentage_laid_off =  '' ;


select *
from layoffs_staging2;


alter table layoffs_staging2
drop column row_num;

















