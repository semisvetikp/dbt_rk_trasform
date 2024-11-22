-- Use the `ref` function to select from other models
with tbl as (
select 11 as id, 'AA' as value_txt
union
select 111 as id, 'AAA' as value_txt
) 
select tbl.id,
tbl.value_txt,
 "devdb"."dwh_sbx"."psa_test".name_name as value_txt_2
 from tbl
left join "devdb"."dwh_sbx"."psa_test"
on tbl.id = "devdb"."dwh_sbx"."psa_test".id