/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

select
    cat.category_id     category_id,
    cat.name            category_name,
    count(cat.*)        total_films_count
from category cat
left join film_category fc on
    cat.category_id = fc.category_id
group by cat.category_id
order by total_films_count desc



/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

select
    a.actor_id,
    a.first_name,
    a.last_name,
    count(fa.*)     total_film_count
from film_actor fa
inner join actor a on a.actor_id = fa.actor_id
group by a.actor_id
order by total_film_count desc
limit 10



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

select
    cat.category_id             category_id,
    cat.name                    category_name,
    sum(f.replacement_cost)     sum_production_cost    -- not sure this field was correctly chosen
from category cat
inner join film_category fc on
    cat.category_id = fc.category_id
inner join film f on
    fc.film_id = f.film_id
group by cat.category_id
order by sum_production_cost desc
limit 1



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

select
    f.title
from film f
left join inventory i on f.film_id = i.film_id
where i.inventory_id is null



/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

select
    act.actor_id,
    act.first_name,
    act.last_name,
    count(fa.*)     total_films_count
from actor act
inner join film_actor fa on
    act.actor_id = fa.actor_id
inner join film_category flm_cat on
    fa.film_id = flm_cat.film_id
inner join category cat on
    flm_cat.category_id = cat.category_id
where cat.name = 'Children'
group by act.actor_id
order by total_films_count desc
limit 3



