--14.04.2020
--обработка всех срезов фронтов
--два варианта запросов: по старому и новому формату

--new query for Kotzemir 20.04.2021
select * from 
(
select b.front_name, b.total, b.cited_total,a.rus,a.cited_rus
from
(select front_name,count(distinct accession_number) rus,sum(times_cited::numeric) cited_rus
from cleaned_fronts_jan2021
where accession_number in (select distinct accession_number from cleaned_fronts_jan2021 cfa where countries like '%RUSSIA%')
group by front_name) a
--natural join
right join
(select front_name,count(distinct accession_number) total,sum(times_cited::numeric) cited_total
from cleaned_fronts_jan2021
--where accession_number in (select distinct accession_number from cleaned_fronts_aug2020 cfa where countries like '%RUSSIA%')
group by front_name) b
on a.front_name = b.front_name
order by rus desc
) ab
natural join 
--witch author's countries are represented in a front
(select front_name, array_agg(distinct cntry) cntrys--count(cntry)
from (
select front_name, accession_number, authors ,unnest(string_to_array(countries,';')) cntry
from cleaned_fronts_jan2021
--where accession_number in (select distinct accession_number from cleaned_fronts_jan2021 cfa where countries like '%RUSSIA%')
) a
group by front_name
) c

--статьи по странам old format общий файл на все срезы
--create materialized view _0_cnt_fronts_by_cntry as
select 
	srez
	,row_number() over(partition by srez order by front_cnt desc) place
 	,country
	,front_cnt
from (
	select srez,country,front_cnt from
	(
	with rus_f as (
	select to_char(wm.date,'YYYY Month') srez, name,country, wmp.wos_id_int, wmp.citation_count, id_front
	from wos_measurements_papers wmp
	join wos_measurements wm on wmp.id_measurment = wm.id
	join (
		select upper(country) country, wos_id_int 
		from wos_papers_affil
		--where lower(country) like '%russia%'
		where country is not null and country <> '' and upper(country) not in ('ENGLAND', 'SCOTLAND', 'WALES', 'NORTHERN IRELAND')
	) wpa on wmp.wos_id_int = wpa.wos_id_int 
	join wos_fronts wf on wf.id = wmp.id_front and wf.id_measurment = wmp.id_measurment 
	)
	select 
		srez--,id_front
		,country
		,count(distinct name) front_cnt
		--, count(distinct wos_id_int) cnt_rus_art, sum(citation_count::numeric) sum_rus_cited
	from rus_f
	group by srez,country
	) rus
	union
	select srez,country,front_cnt from
	(
	with rus_f as (
	select to_char(wm.date,'YYYY Month') srez, name,country, wmp.wos_id_int, wmp.citation_count, id_front
	from wos_measurements_papers wmp
	join wos_measurements wm on wmp.id_measurment = wm.id
	join (
		select 'UK' country, wos_id_int 
		from wos_papers_affil
		--where lower(country) like '%russia%'
		where country is not null and country <> '' and upper(country) in ('ENGLAND', 'SCOTLAND', 'WALES', 'NORTHERN IRELAND')
	) wpa on wmp.wos_id_int = wpa.wos_id_int 
	join wos_fronts wf on wf.id = wmp.id_front and wf.id_measurment = wmp.id_measurment 
	)
	select 
		srez--,id_front
		,country
		,count(distinct name) front_cnt
		--, count(distinct wos_id_int) cnt_rus_art, sum(citation_count::numeric) sum_rus_cited
	from rus_f
	group by srez,country
	) uk
) all_uk
--
--итоговая таблица по странам, старый формат
select srez,place,country,front_cnt,fronts_cnt as fronts_by_srez_cnt
from _0_cnt_fronts_by_cntry
natural join 
(select to_char(wm.date,'YYYY Month') srez,count(distinct id_front) fronts_cnt
from wos_measurements_papers wmp
join wos_measurements wm on wmp.id_measurment = wm.id
group by wm.date) total
order by srez asc,place asc, front_cnt desc  


--
--общая информация, старый формат
select *
from 
(
select to_char(wm.date,'YYYY Month') srez,count(distinct id_front) fronts_cnt, count(distinct wos_id) art_cnt
from wos_measurements_papers wmp
join wos_measurements wm on wmp.id_measurment = wm.id
group by wm.date) a
natural left join
(select to_char(wm.date,'YYYY Month') srez, count(distinct wpa.wos_id_int) rus_art_cnt, count(distinct id_front) rus_fronts_cnt
from wos_measurements_papers wmp
join wos_measurements wm on wmp.id_measurment = wm.id
join wos_papers_affil wpa on wmp.wos_id_int = wpa.wos_id_int 
where lower(country) like '%russia%'
group by wm.date
) b
--
-- фронты с рус участием, старый формат. количество рус статей, сумма рус цитирований, количество всего статей в рус фронте, сумма всего цитирований в рус фронтах
select * from
(
with rus_f as (
select to_char(wm.date,'YYYY Month') srez, name, wmp.wos_id_int, wmp.citation_count, id_front
from wos_measurements_papers wmp
join wos_measurements wm on wmp.id_measurment = wm.id
join (
	select distinct wos_id_int 
	from wos_papers_affil
	where lower(country) like '%russia%'
) wpa on wmp.wos_id_int = wpa.wos_id_int 
join wos_fronts wf on wf.id = wmp.id_front and wf.id_measurment = wmp.id_measurment 
)
select 
	srez--,id_front
	,name
	, count(distinct wos_id_int) cnt_rus_art, sum(citation_count::numeric) sum_rus_cited
from rus_f
group by srez,name
) rus
natural left join 
(
with total_f as (
select to_char(wm.date,'YYYY Month') srez, name, wmp.wos_id_int, wmp.citation_count, id_front
from wos_measurements_papers wmp
join wos_measurements wm on wmp.id_measurment = wm.id
join wos_fronts wf on wf.id = wmp.id_front and wf.id_measurment = wmp.id_measurment 
)
select 
	srez--,id_front
	,name
	, count(distinct wos_id_int) cnt_total_art, sum(citation_count::numeric) sum_total_cited
from total_f
group by srez,name
) total
order by srez asc,cnt_rus_art desc
--
--
--статьи по странам, новый формат
with by_cntr as (
select cntr, count(distinct id_front) front_cnt
from cleaned_fronts_jan2020, unnest(string_to_array(countries, ';')) cntr
where cntr not in ('ENGLAND', 'SCOTLAND', 'WALES', 'NORTHERN IRELAND','')
group by cntr
union ALL
select 'UK', count(distinct front_name)
from cleaned_fronts_jan2020, unnest(string_to_array(countries, ';')) pam
where pam in ('ENGLAND', 'SCOTLAND', 'WALES', 'NORTHERN IRELAND')
)
select row_number() over(order by front_cnt desc) place,
cntr, front_cnt, 10996 as total, front_cnt/10996::numeric*100 prcnt 
from by_cntr 
order by front_cnt desc


--фронты с росс участием
with art_rus as (
select *, case when lower(countries) like '%russia%' then 1 end as "is_rus"
from cleaned_fronts_jan2020
)
select * from
(select 
front_name, count(accession_number) cnt_rus_art
,sum(times_cited::numeric) sum_rus_cited
from art_rus 
where is_rus = 1
group by front_name) rus
natural left join 
(select 
front_name, count(accession_number) cnt_total_art
,sum(times_cited::numeric) sum_total_cited
from art_rus
group by front_name) total
--on rus.front_name=total.front_name
order by sum_rus_cited desc 
--
-- статьи по областям, старый формат
--
--фин таблица
select * from
(select 
	--wmp.id_measurment
	to_char(date,'YYYY Month') srez
	--,id_field
	,wfi.title_rus 
	,wfi.title
	,count(distinct wmp.wos_id_int) cnt_total_art
from wos_measurements_papers wmp
join (select wos_id_int, case when lower(country) like '%russia%' then 1 end as is_rus
	from
	(select wos_id_int,string_agg(country,';') country
	from wos_papers_affil 
	group by wos_id_int ) wpa 
) a
on wmp.wos_id_int = a.wos_id_int
left join wos_papers wp on wmp.wos_id_int = wp.wos_id_int 
join wos_fields wfi on wp.id_field=wfi.id
join wos_measurements wm on wmp.id_measurment = wm.id
group by 
	--wmp.id_measurment
	wm.date
	--,id_field
	,wfi.title_rus 
	,wfi.title) y
natural left join
(
select 
	--wmp.id_measurment
	to_char(date,'YYYY Month') srez
	--,id_field
	,wfi.title_rus 
	,wfi.title
	,count(distinct wmp.wos_id_int) cnt_rus_art
from wos_measurements_papers wmp
join (select wos_id_int, case when lower(country) like '%russia%' then 1 end as is_rus
	from
	(select wos_id_int,string_agg(country,';') country
	from wos_papers_affil 
	group by wos_id_int ) wpa 
) a
on wmp.wos_id_int = a.wos_id_int
left join wos_papers wp on wmp.wos_id_int = wp.wos_id_int 
join wos_fields wfi on wp.id_field=wfi.id
join wos_measurements wm on wmp.id_measurment = wm.id
where is_rus = 1
group by 
	--wmp.id_measurment
	wm.date
	--,id_field
	,wfi.title_rus 
	,wfi.title
) x	


-- фронты по областям, старый формат
--
--create materialized view cnt_rus_fronts_by_researchfield as
with all_fronts as (
select name,title,title_rus,date
	, sum(is_rus) is_rus_f
	, count(wos_id_int) as cnt_art
from 
(select wos_id_int, case when lower(country) like '%russia%' then 1 end as is_rus
from wos_papers_affil wpa 
) a
natural join 
(select wos_fronts.id as id_front, id_field,wos_fronts.id_measurment, wos_id_int, name
from wos_papers
join wos_fronts on wos_papers.id_front = wos_fronts.id 
) wp
join wos_fields wfi on wp.id_field=wfi.id
join wos_measurements wm on wp.id_measurment = wm.id
group by date,wfi.title_rus,wfi.title,name
)
select to_char(date,'YYYY Month') srez,title_rus,title, count(distinct name) cnt_rus_fronts
from all_fronts 
where is_rus_f = 1
group by date,title_rus,title
--
CREATE INDEX  wpa_country  ON wos_papers_affil(country)
--
--create materialized view cnt_total_fronts_by_researchfield as
with all_fronts as (
select name,title,title_rus,date
	, sum(is_rus) is_rus_f
	, count(wos_id_int) as cnt_art
from 
(select wos_id_int, case when lower(country) like '%russia%' then 1 end as is_rus
from wos_papers_affil wpa 
) a
natural join 
(select wos_fronts.id as id_front, id_field,wos_fronts.id_measurment, wos_id_int, name
from wos_papers
join wos_fronts on wos_papers.id_front = wos_fronts.id 
) wp
join wos_fields wfi on wp.id_field=wfi.id
join wos_measurements wm on wp.id_measurment = wm.id
group by date,wfi.title_rus,wfi.title,name
)
select to_char(date,'YYYY Month') srez,title_rus,title, count(distinct name) cnt_total_fronts
from all_fronts 
group by date,title_rus,title
--
--финальная таб
select * 
from cnt_total_fronts_by_researchfield t
natural left join cnt_rus_fronts_by_researchfield r
order by srez asc,title


---

--фронты по областям 
with rf as (
select *, case when lower(countries) like '%russia%' then 1 end as "is_rus"
from cleaned_fronts_jan2020
)
select * from
(select 
research_field, count(distinct id_front) cnt_total_fronts
from rf 
group by research_field) total
natural left join 
(select 
research_field, count(distinct id_front) cnt_rus_fronts
from rf
where is_rus = 1 
group by research_field) rus
order by research_field

--статьи по областям 
with rf as (
select *, case when lower(countries) like '%russia%' then 1 end as "is_rus"
from cleaned_fronts_jan2020
)
select * from
(select 
research_field, count(distinct accession_number) cnt_total_fronts
from rf 
group by research_field) total
natural left join 
(select 
research_field, count(distinct accession_number) cnt_rus_fronts
from rf
where is_rus = 1 
group by research_field) rus
order by research_field
--

--общее	'Jan20'
with rus as (
select *, case when lower(countries) like '%russia%' then 1 end as "is_rus"
from fronts.cleaned_fronts_jan2020
)
select 
	count(distinct id_front)	
from fronts.cleaned_fronts_jan2020
union
select 
	count(distinct id_front)	
from rus
where is_rus = 1
union
select 
	count(distinct accession_number)	
from rus
where is_rus = 1
union
select 
	count(distinct accession_number)	
from rus

--keywords
select *
from
(with rus as (
select *, case when lower(countries) like '%russia%' then 1 end as "is_rus"
from fronts.cleaned_fronts_jan2020
)
select kw,count(*) cnt_kw
from (
	select 
		unnest(string_to_array(front_name,';')) kw
	from rus) kwt
group by kw) a	
natural left join
(with rus as (
select *, case when lower(countries) like '%russia%' then 1 end as "is_rus"
from fronts.cleaned_fronts_jan2020
)
select kw,count(*) cnt_kw_ru
from (
	select 
		unnest(string_to_array(front_name,';')) kw
	from rus
	where is_rus = 1) kwt
group by kw
) b
order by cnt_kw desc
--
--

select *
from fronts.cleaned_fronts_jan2020 cfj 
where front_name = '19 SOLAR-MASS BINARY BLACK HOLE COALESCENCE;22-SOLAR-MASS BINARY BLACK HOLE COALESCENCE;50-SOLAR-MASS BINARY BLACK HOLE COALESCENCE;BINARY BLACK HOLE COALESCENCE;BINARY BLACK HOLE MERGER'

--new query for Kotzemir (см письмо от 08.04.2021)
select *
from
(select front_name,count(distinct accession_number) rus,sum(times_cited::numeric) cited_rus
from cleaned_fronts_aug2020
where accession_number in (select distinct accession_number from cleaned_fronts_aug2020 cfa where countries like '%RUSSIA%')
group by front_name) a
--natural join
right join
(select front_name,count(distinct accession_number) total,sum(times_cited::numeric) cited_total
from cleaned_fronts_aug2020
--where accession_number in (select distinct accession_number from cleaned_fronts_aug2020 cfa where countries like '%RUSSIA%')
group by front_name) b
on a.front_name = b.front_name
order by rus desc
