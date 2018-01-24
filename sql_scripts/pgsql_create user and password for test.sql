-- User: user_name
-- DROP USER user_name;

CREATE USER user_name WITH
  LOGIN
  NOSUPERUSER
  INHERIT
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION;

GRANT postgres TO user_name;