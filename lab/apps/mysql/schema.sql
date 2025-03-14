CREATE DATABASE IF NOT EXISTS vendas;
CREATE USER IF NOT EXISTS 'big_data_importer'@'%' IDENTIFIED BY 'big_data_importer' WITH MAX_USER_CONNECTIONS 3;
USE vendas;
CREATE TABLE IF NOT EXISTS vendedores (
  id_vendedor INT NOT NULL AUTO_INCREMENT,
  cpf VARCHAR(14),
  telefone VARCHAR(15),
  email VARCHAR(60),
  origem_racial VARCHAR(10),
  nome VARCHAR(50),
  criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  atualizacao DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_vendedor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS produtos (
  id_produto INT NOT NULL AUTO_INCREMENT,
  produto VARCHAR(100),
  preco DECIMAL(10,2),
  criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  atualizacao DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_produto)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS clientes (
  id_cliente INT NOT NULL AUTO_INCREMENT,
  cpf VARCHAR(14),
  telefone VARCHAR(15),
  email VARCHAR(60),
  cliente VARCHAR(50),
  estado VARCHAR(2),
  origem_racial VARCHAR(10),
  sexo CHAR(1),
  status VARCHAR(50),
  criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  atualizacao DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_cliente)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS vendas (
  id_venda INT NOT NULL AUTO_INCREMENT,
  id_vendedor INT,
  id_cliente INT,
  data DATE,
  total DECIMAL(10,2),
  criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  atualizacao DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_venda),
  FOREIGN KEY (id_vendedor) REFERENCES vendedores(id_vendedor),
  FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS itens_venda (
  id_produto INT NOT NULL,
  id_venda INT NOT NULL,
  quantidade INT,
  valor_unitario DECIMAL(10,2),
  valor_total DECIMAL(10,2),
  desconto DECIMAL(10,2),
  criacao DATETIME DEFAULT CURRENT_TIMESTAMP,
  atualizacao DATETIME DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_produto, id_venda),
  FOREIGN KEY (id_produto) REFERENCES produtos(id_produto) ON DELETE RESTRICT,
  FOREIGN KEY (id_venda) REFERENCES vendas(id_venda) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE INDEX vendedores_criacao ON vendedores (criacao, atualizacao);
CREATE INDEX produtos_criacao ON produtos (criacao, atualizacao);
CREATE INDEX clientes_criacao ON clientes (criacao, atualizacao);
CREATE INDEX vendas_criacao ON vendas (criacao);
CREATE INDEX itens_venda_criacao ON itens_venda (criacao);

delimiter //

CREATE PROCEDURE USER_INFO(IN LAST_DATE DATETIME, IN TB_NAME VARCHAR(50))
BEGIN
  SET @ID = CASE
			WHEN TB_NAME = "vendedores" THEN "id_vendedor"
			WHEN TB_NAME = "produtos" THEN "id_produto"
			WHEN TB_NAME = "clientes" THEN "id_cliente"
			ELSE "id_venda" END;

	SET @q = CONCAT('SELECT * FROM
		(SELECT max_user_connections AS MAX_CONN FROM mysql.user WHERE user = SUBSTRING_INDEX(USER(),"@",1)) AS MAX_CONN,',
		'(SELECT MIN(',@ID,') AS "LOW_BOUND" FROM ', TB_NAME, ' WHERE criacao > "', LAST_DATE,'" OR atualizacao > "', LAST_DATE,'") AS LOW_BOUND,',
		'(SELECT MAX(',@ID,') AS "UPPER_BOUND" FROM ', TB_NAME, ' WHERE criacao > "', LAST_DATE,'" OR atualizacao > "', LAST_DATE,'") AS UPPER_BOUND,',
		'(SELECT COUNT(*) AS "QTD_LINHAS" FROM ', TB_NAME, ' WHERE criacao > "', LAST_DATE,'" OR atualizacao > "', LAST_DATE,'") AS QTD_LINHAS;'
	);
    PREPARE stmt FROM @q;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

delimiter //

CREATE PROCEDURE EXPORT_DATA(IN LAST_DATE DATE, IN TB_NAME VARCHAR(50))
BEGIN
	SET @q = CONCAT('(SELECT * FROM ', TB_NAME, ' WHERE criacao > "', LAST_DATE,'" OR atualizacao > "', LAST_DATE,'")');
    PREPARE stmt FROM @q;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

GRANT SELECT ON vendas.* TO 'big_data_importer'@'%';
GRANT EXECUTE ON PROCEDURE USER_INFO TO 'big_data_importer'@'%';
GRANT EXECUTE ON PROCEDURE EXPORT_DATA TO 'big_data_importer'@'%';
FLUSH PRIVILEGES;