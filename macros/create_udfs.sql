{#
Defines useful UDFs :
- lenient cast / conversion

To use them, they have to be prefixed by the current schema (because they are created here, else no rights),
and arguments must have the exact right type, ex. :
{{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}DateConstruction__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text)

Must be called as a DBT pre-hook.
Big UDFs should be defined in their own files and only referenced there.
Follows the principles described at :
https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18
#}

{% macro create_udfs() %}

SELECT pg_catalog.set_config('search_path', '{{target.schema}}', false); -- because no rights for create schema if not exists {{target.schema}};

create or replace function to_date_or_null (s text, fmts VARIADIC text[])
  returns date
as $$
DECLARE
  fmt text;
  d date;
begin
  IF s is NULL or length(trim(s)) = 0 THEN
    return NULL;
  END IF;
  FOREACH fmt IN ARRAY fmts
  LOOP
    begin
      d := to_date(s, fmt);
      IF d IS NOT NULL THEN
        return d;
      END IF;
    exception
      when others then -- do nothing, loop
    end;
  END LOOP;
  return NULL;
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_numeric_or_null (s text)
  returns numeric
as $$
begin
  return cast(s as numeric);
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

create or replace function to_boolean_or_null (s text)
  returns boolean
as $$
begin
  return cast(s as boolean); -- incl. null
exception
  when others then
  begin
    return case trim(s::text) when 'oui' then true else false end; -- trim accepts null
  exception
    when others then return null;
  end;
end;
$$ language plpgsql;

{% endmacro %}