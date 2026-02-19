CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_vendedor (
    sk_vendedor SERIAL PRIMARY KEY,
    idpessoavendedor INT,
    vendedor TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_administradora (
    sk_administradora SERIAL PRIMARY KEY,
    idconsorcioadministradorabase INT,
    administradora TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_tempo (
    sk_tempo SERIAL PRIMARY KEY,
    datavenda DATE,
    ano INT,
    mes INT,
    dia INT
);

CREATE TABLE IF NOT EXISTS gold.fato_vendas (
    sk_vendedor INT,
    sk_administradora INT,
    sk_tempo INT,
    valorcredito NUMERIC,
    valorprimeiraparcela NUMERIC,
    quantidade INT,
    hash_linha TEXT
);
