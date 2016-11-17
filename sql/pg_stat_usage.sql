create extension pg_stat_usage;

begin;

create table t (
i serial primary key
);

insert into t select * from generate_series(1, 1000);

create or replace function foo() returns void as
$$
begin
	perform bar();
end;
$$ language plpgsql;

create or replace function bar() returns void as
$$
begin
	perform baz();
	perform qux();
end;
$$ language plpgsql;

create or replace function baz() returns void as
$$
begin
	perform qux();
	perform qux();
end;
$$ language plpgsql;

create or replace function qux() returns void as
$$
begin
	perform count(*) from t;
end;
$$ language plpgsql;

select foo();

end;

-- allow some time for the stats to process
select pg_sleep(1);

select object_type, object_schema, object_name, num_calls, num_scans from pg_stat_usage
order by 1, 2, 3;

