{#
Defines useful UDFs :
- lenient cast / conversion

Must be called as a DBT pre-hook.
Big UDFs should be defined in their own files and only referenced there.
Follows the principles described at :
https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18
#}

{% macro create_udfs() %}

SELECT pg_catalog.set_config('search_path', '{{target.schema}}', false); -- because no rights for create schema if not exists {{target.schema}};

create or replace function to_date_or_null (s text, fmt text)
  returns date
as $$
begin
  return to_date(s, fmt);
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_decimal_or_null (s text)
  returns decimal
as $$
begin
  return cast(s as decimal);
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_integer_or_null (s text)
  returns integer
as $$
begin
  return cast(s as integer);
exception
  when others then return null;
end;
$$ language plpgsql;

{% endmacro %}