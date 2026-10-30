-- Fails if a direct identifier column ever shows up in the gold schema.
select table_name, column_name
from information_schema.columns
where table_schema = 'gold'
  and column_name in ('member_id', 'first_name', 'last_name', 'email', 'phone',
                      'date_of_birth', 'zip_code')
