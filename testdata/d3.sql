-- name: CreateTable
create table person(id varchar(100), age int);

-- name: AddAll
insert into person(id, age) values(':id', ':age');

-- name: ListAll
select id, age from person order by id;

