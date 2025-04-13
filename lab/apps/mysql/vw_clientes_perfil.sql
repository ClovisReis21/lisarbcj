CREATE VIEW vw_clientes_perfil AS
SELECT 
  id_cliente,
  cliente,
  estado,
  sexo,
  origem_racial,
  status,
  YEAR(criacao) AS ano_entrada
FROM clientes;