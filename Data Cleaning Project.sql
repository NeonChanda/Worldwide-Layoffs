select *
from layoffs;

# we do not want to make any permanent chnages to the raw data so it would be a good idea to create a duplicate here

create table layoffs_2
like layoffs;

insert layoffs_2
select *
from layoffs;

# remove duplicate values

select *, 
row_number() over(partition by
company, industry, total_laid_off, percentage_laid_off)
from layoffs_2;

# no row values were present this gives them all a unique value of 1. Anything more than 1 is a duplicate

with duplicate_remove_cte as
(
select *,
row_number() over( partition by
company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_2
)
select *
from duplicate_remove_cte
where row_num > 1;

CREATE TABLE `layoffs_3_remove_dupes` (
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


insert into layoffs_3_remove_dupes
select *,
row_number() over( partition by
company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_2;

select *
from layoffs_3_remove_dupes
where row_num > 1;

delete
from layoffs_3_remove_dupes
where row_num > 1;

select *
from layoffs_3_remove_dupes;

# standardisation of data

select distinct (trim(company))
from layoffs_3_remove_dupes;

update layoffs_3_remove_dupes
set company = trim(company);

select *
from layoffs_3_remove_dupes;

select distinct industry
from layoffs_3_remove_dupes;

update layoffs_3_remove_dupes
set industry = 'Crypto'
where industry like 'Crypto%' ;

select distinct industry
from layoffs_3_remove_dupes;

select distinct location
from layoffs_3_remove_dupes
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_3_remove_dupes
order by 1;

update layoffs_3_remove_dupes
set country = trim(trailing '.' from country)
where country like 'United States%' ;
 
select distinct country
from layoffs_3_remove_dupes
order by 1;

# it has just come to my attention that the date column is a string not a date

select date,
str_to_date( `date`, '%m/%d/%Y')
from layoffs_3_remove_dupes;

update layoffs_3_remove_dupes
set `date` = str_to_date( `date`, '%m/%d/%Y')
;

select *
from layoffs_3_remove_dupes
;

alter table layoffs_3_remove_dupes
modify column `date` date;

# Null and Blank Values

# deleting null values is a last resort and should only really be done when multiple columns are missing for that particular datapoint.
# if we have something we can macth by then we should use it to fill in the data.

update layoffs_3_remove_dupes
set industry = Null
where industry = '' ;


select *
from layoffs_3_remove_dupes t1
join layoffs_3_remove_dupes t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null
;

update layoffs_3_remove_dupes t1
join layoffs_3_remove_dupes t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null
;

# removing columns and rows with more than one missing rows/columns

select *
from layoffs_3_remove_dupes
where total_laid_off is null
and percentage_laid_off is null
;

# all of these values have two missing columns and there is no company size within this dataset to allow us to calculate the % laid off


delete
from layoffs_3_remove_dupes
where total_laid_off is null
and percentage_laid_off is null
;

alter table layoffs_3_remove_dupes
drop column row_num;

select *
from layoffs_3_remove_dupes;

select *
from layoffs_3_remove_dupes
where company like 'Bally%';

delete
from layoffs_3_remove_dupes
where company like 'Bally%';

