/*
Get the rate of the the quantity of people who responded the question over all, by title (movie), I use duckdb
*/
with table_a as (
    select
    title,
    no_answer,
    count(answers) q_answers,
    from df
    where len(title)>8
    group by 1,2
    order by 1,2)
select distinct
title, rate
from
(
    select
    title,
    no_answer,
    q_answers,
    count_per_title,
    round(max(case when no_answer=False then q_answers end) over (partition by title)/
    (max(case when no_answer=True then q_answers end) over (partition by title)+max(case when no_answer=False then q_answers end) over (partition by title) ) ,2) as rate
    from (
        select
        title,
        no_answer,
        q_answers,
        count(no_answer) over(partition by title) as count_per_title
        from table_a )
    where count_per_title>1
    group by 1,2,3,4
    order by 1,2,3,4) 
order by 2 asc
limit 10