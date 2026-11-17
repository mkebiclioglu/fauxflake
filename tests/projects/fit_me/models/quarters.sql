-- a list the project works out itself (no source, no seed): the quarters it reports on
select column1 as quarter_name, column2 as starts_on
from values ('FY24-Q1', '2023-02-01'::date), ('FY24-Q2', '2023-05-01'::date), ('FY24-Q3', '2023-08-01'::date)
