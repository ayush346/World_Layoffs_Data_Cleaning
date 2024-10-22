-- data cleaning

select *
from world_layoffs.layoffs;

USE world_layoffs;
  create table layoffs_staging
  like world_layoffs.layoffs ;

select *                              # now we have the coloumns all we have to do is inserting the data
from world_layoffs.layoffs_staging;

insert layoffs_staging   # here we will insert the data to newly created table , so that we wont harm the raw data for safety purposes 
select *
from world_layoffs.layoffs;

select *,
  ROW_NUMBER() OVER(
  PARTITION BY company, industry, total_laid_off,percentage_laid_off,`date`) as row_num # by adding row number we can identify the duplicates 
  from world_layoffs.layoffs_staging;
  
  with duplicate_cte as
  (
  select *,
  ROW_NUMBER() OVER(
  PARTITION BY company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num 
  from world_layoffs.layoffs_staging  # by adding row number we can identify the duplicates 
  )
  select *
  from duplicate_cte  # this will display the duplicates
  where row_num > 1;
  
  select *
  from  layoffs_staging     # this will display all the rows with company named 'casper' this will help us to identify the duplicates
  where company = 'Casper';
  
  # here we are creating the third table directly by right clicking in layoffs_staging-copy to clipboard-create statement
   
  drop table if exists layoffs_staging2;
  CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` int                                   # we will add this coloumn manually so that layoffs_staging exactly fits in it
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from world_layoffs.layoffs_staging2  # this are the duplicates row that we want to delete 
where row_num > 1;

insert into world_layoffs.layoffs_staging2
select *,
  ROW_NUMBER() OVER(
  PARTITION BY company, industry, total_laid_off,percentage_laid_off,`date`) as row_num # by adding row number we can identify the duplicates 
  from world_layoffs.layoffs_staging;
  
SET SQL_SAFE_UPDATES = 0;
  
delete 
from world_layoffs.layoffs_staging2  # here we are deleting the duplicate rows
where row_num > 1;

select company
from world_layoffs.layoffs_staging2
where company = 'Amazon';

-- Standardizing data
   select company , trim(company)
   from world_layoffs.layoffs_staging2;
   
   update world_layoffs. layoffs_staging2
   set company = trim(company);
   
   select distinct industry
   from world_layoffs.layoffs_staging2
   order by 1;                             # here one refers to first coloumn
  
  select *
  from world_layoffs.layoffs_staging2
  where industry like 'crypto%';
  
  update world_layoffs.layoffs_staging2
  set industry = 'Crypto'            # here we ordered all the Crypto currency or Crypto Currency name to 'Crypto'
  where industry like 'Crypto%';
  
  select distinct location
  from world_layoffs.layoffs_staging2
  order by 1;
  
  select distinct location
  from world_layoffs.layoffs_staging2
  order by 1;
  
  select *
  from world_layoffs.layoffs_staging2
  where country like 'United States%'
  order by 1;
  
  select distinct country, trim(trailing '.' from country) # trailing means by the end. trim means removing
  from world_layoffs.layoffs_staging2
  where country like 'United States%'    
  order by 1;
  
  update world_layoffs.layoffs_staging2
  set country = trim(trailing '.' from country)
  where country like 'united states%'  ;
  
  select *
  from world_layoffs.layoffs_staging2; 
  
  select `date`,  # here date is in a text format so we convert it into time series
  str_to_date(`date`,'%m/%d/%Y')# this converts string to date that is to time series. But it actually doesnt change the data type it changes the format
  from world_layoffs.layoffs_staging2;
  
  update world_layoffs. layoffs_staging2
  set `date` = str_to_date(`date`,'%m/%d/%Y') ;
 
 alter table layoffs_staging2 # here we actually the change the data type from text ro date
 modify column `date` DATE; 
  
  select *
  from world_layoffs.layoffs_staging2;
 
 -- null values or blank values
  select *
  from world_layoffs.layoffs_staging2
  where total_laid_off is null
  and percentage_laid_off is null;
  
  select *
  from world_layoffs.layoffs_staging2
  where industry is null 
  or industry = '';
  
  select *
  from world_layoffs.layoffs_staging2    # after performimg all the below queries u run this rows with null values in industry table will be filled
  where company = 'Airbnb';
  
  update world_layoffs. layoffs_staging2   #here we are updating all the '' values to null
  set industry = null
  where industry = '';
  
  select *
  from world_layoffs.layoffs_staging2 t1
  join world_layoffs.layoffs_staging2 t2
    on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is NULL or t1.industry = '')
and   t2.industry is not null;

select t1.industry , t2.industry
  from world_layoffs.layoffs_staging2 t1
  join world_layoffs.layoffs_staging2 t2           # this actually want to replace all the blank statement of t1 by the help of t2 where we specified perfectly
    on t1.company = t2.company                     # but it wont to fix this issue
    and t1.location = t2.location                  # we want to update all the blank statements to null
where (t1.industry is NULL or t1.industry = '')    # as we did above, scroll up little bit to see it
and   t2.industry is not null;

update world_layoffs.layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2. industry is not null;

select *
from layoffs_staging2;

select *
from world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
 
 delete
 from world_layoffs.layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 select*
 from world_layoffs.layoffs_staging2;
  
  alter table world_layoffs.layoffs_staging2
  drop column row_num;
  
-- Exploratory data analysis

select *
from world_layoffs.layoffs_staging2;
  
select max( total_laid_off), max(percentage_laid_off) # here max(percentage_laid_off) = 1 which means entire employees were laid off
from world_layoffs.layoffs_staging2;

select *
from world_layoffs.layoffs_staging2
where percentage_laid_off =1
order by funds_raised_millions desc;
  
select company,sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by company
order by 2 desc;
  
select min(`date`),max(`date`)
from world_layoffs.layoffs_staging2;

select industry,sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by industry
order by 2 desc;
  
select year(`date`),sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by year(`date`)
order by 1 desc;
  
select stage,sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by stage
order by 2 desc ;

select substring(`date` ,6,2) as `MONTH`,sum(total_laid_off)
from world_layoffs.layoffs_staging2
where substring(`date` ,6,2) is not null
group by `MONTH`
order by 1;

select substring(`date` ,1,7) as `MONTH`,sum(total_laid_off)
from world_layoffs.layoffs_staging2
where substring(`date` ,1,7) is not null
group by `MONTH`
order by 1;

with rolling_total as
(
select substring(`date` ,1,7) as `MONTH`, sum(total_laid_off) as total_layoff
from world_layoffs.layoffs_staging2
where substring(`date` ,1,7) is not null
group by `MONTH`
order by 1
)
select `month`,total_layoff ,
sum(total_layoff) over(order by `Month`)as sum_rolling_out
from rolling_total
group by `MONTH`;
  
select company, year(`DATE`),sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by company,year(`date`)
order by 3 desc;

with Company_year (Company , Years , total_laid_off) as  # inside the bracket wwe named the column name
(
select company, year(`DATE`),sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by company,year(`date`)
order by 3 desc
),company_year_rank as
(select *,dense_rank() over(partition by years order by total_laid_off desc) as ranking
from Company_year
where years is not null 
)
select *
from company_year_rank
where ranking <= 5;

select *
from world_layoffs.layoffs;




  
  
  
  
  
  






