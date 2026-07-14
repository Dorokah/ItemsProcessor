
table_name = 'pokemon'
families = ['info', 'name', 'type', 'base', 'profile', 'evolution']

if exists table_name
  disable table_name
  drop table_name
end

create table_name, *families
echo "finished"