#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE ROLE handle NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN PASSWORD 'passwd';
    CREATE DATABASE handle OWNER handle ENCODING 'utf-8';
    GRANT ALL PRIVILEGES ON DATABASE handle TO handle;
EOSQL

psql -v ON_ERROR_STOP=1 --username "handle" <<-EOSQL
    CREATE TABLE nas (
      na bytea not null,
      primary key(na)
    );
    CREATE TABLE handles (
      handle bytea not null,
      idx int4 not null,
      type bytea, data bytea,
      ttl_type int2,
      ttl int4,
      timestamp int4,
      refs text,
      admin_read bool,
      admin_write bool,
      pub_read bool,
      pub_write bool,
      primary key(handle, idx)
    );

    CREATE INDEX dataindex on handles ( data );
    CREATE INDEX handleindex on handles ( handle );

    GRANT ALL ON nas,handles to handle;
    GRANT SELECT ON nas,handles TO public;

    INSERT INTO handles
    VALUES  (convert_to('TEST/ADMIN','UTF8'),100,convert_to('HS_ADMIN','UTF8'),convert_to('TEST/ADMIN','UTF8'),0,86400,1533122317,convert_to('','UTF8'),TRUE,TRUE,TRUE,FALSE),
            (convert_to('TEST/ADMIN','UTF8'),300,convert_to('HS_SECKEY','UTF8'),convert_to('ASECRETKEY','UTF8'),0,86400,1533122431,convert_to('','UTF8'),TRUE,TRUE,TRUE,TRUE);
    INSERT INTO nas VALUES ('0.NA/TEST'), ('TEST');
EOSQL
