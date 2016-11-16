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
	null;
end;
$$ language plpgsql;
