# EDA Exploratory Data Analysis

select * 
from layoffs_3_remove_dupes
;

select min(`date`) as start_date, max(`date`) as end_date
from layoffs_3_remove_dupes;
# the data within this dataset starts on the 11th March 2020 and ends on the 6th of March 2023 meaning we have roughly 3 years worth of data


select company, sum(total_laid_off)
from layoffs_3_remove_dupes
group by company
order by sum(total_laid_off) desc;


select industry, sum(total_laid_off)
from layoffs_3_remove_dupes
group by industry
order by sum(total_laid_off) desc;


select location, sum(total_laid_off)
from layoffs_3_remove_dupes
group by location
order by sum(total_laid_off) desc;


select country, sum(total_laid_off)
from layoffs_3_remove_dupes
group by country
order by sum(total_laid_off) desc;


select substring(`date`, 1,7), sum(total_laid_off)
from layoffs_3_remove_dupes
group by substring(`date`, 1,7)
order by sum(total_laid_off) desc;

select year(`date`) as Years, sum(total_laid_off)
from layoffs_3_remove_dupes
group by Years 
order by 1 desc;

select substring(`date`, 1,7) as Month, sum(total_laid_off)
from layoffs_3_remove_dupes
where substring(`date`, 1,7) is not null
group by substring(`date`, 1,7)
order by 1 asc;

with Rolling_Total as
(
select substring(`date`, 1,7) as Month, sum(total_laid_off) as total_lay_offs
from layoffs_3_remove_dupes
where substring(`date`, 1,7) is not null
group by substring(`date`, 1,7)
order by 1 asc
)

select Month,total_lay_offs,sum(total_lay_offs) over(order by Month) as Rolling_total_layoffs_per_month
from Rolling_Total;


select company, year(`date`), sum(total_laid_off)
from layoffs_3_remove_dupes
group by company, year(`date`)
order by 3 desc;

with Company_Year (Company, Years, Total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_3_remove_dupes
group by company, year(`date`)
order by 3 desc
), Company_Year_Rank as
(select *, dense_rank() over(partition by Years order by Total_laid_off desc) as Ranking
from Company_Year
where Years is not null
)
select * 
from Company_Year_Rank
where Ranking <= 5
;