wget https://ftp.postgresql.org/pub/source/v16.3/postgresql-16.3.tar.gz
tar -xzf postgresql-16.3.tar.gz
cd postgresql-16.3
./configure --prefix=/u/z/z/zzheng/Research/catalog-query/build-catalog/postgresql
make && make install

cd ..
rm -r -f postgresql-16.3
rm -f postgresql-16.3.tar.gz

PATH=/u/z/z/zzheng/Research/catalog-query/build-catalog/postgresql/bin:$PATH
export PATH
cd ./postgresql
initdb -D ./data
pg_ctl start -l logfile -D ./data
pg_ctl stop -D ./data
psql -U zzheng -d postgres
# add users
# CREATE ROLE text2sql WITH SUPERUSER LOGIN;
# add database
# createdb -U text2sql data_catalog
