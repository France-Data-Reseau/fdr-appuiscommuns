-- view over CKAN resources
-- to be created in ckan main ckan database (for now manually)
-- used once sync'd in the datastore database by the import.py script
create or replace view fdr_resource as (
with dsex as (
select
    package_id,
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_CAS_USAGE'))[1]) as "fdr_cas_usage", -- "FDR_CAS_USAGE"
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_ROLE'))[1]) as "fdr_role", -- "FDR_ROLE"
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_SOURCE_NOM'))[1]) as "fdr_source_nom", -- "FDR_SOURCE_NOM"
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_TARGET'))[1]) as "fdr_target" -- "FDR_TARGET"
from package_extra dsex
group by dsex.package_id
),
orgex as (
select
    group_id,
    (ARRAY_AGG(orgex.value) FILTER (WHERE orgex.key = 'FDR_SIREN'))[1] as "fdr_siren" -- "FDR_SIREN"
from group_extra orgex
group by orgex.group_id
)
select
r.id, r.name, r.last_modified, r.size, r.format, r.extras, r.url,
ds.id as ds_id, ds.name as ds_name, ds.title as ds_title,
dsex.*,
org.id as org_id, org.name as org_name, org.title as org_title, -- label
orgex.*,
u.id as u_id, u.email as u_email, u.name as u_name
from resource r -- NB. resource has no _extra table but resource.extras can contain json !
join package ds on r.package_id=ds.id
join dsex on ds.id=dsex.package_id
join public.group org on ds.owner_org=org.id
join orgex on org.id=orgex.group_id
join public.user u on ds.creator_user_id=u.id
where
r.state = 'active' and ds.state = 'active'
and org.is_organization -- else mere group
and "fdr_cas_usage" is not null -- "FDR_CAS_USAGE"
--and r.format in ('CSV', 'Excel')
)