CREATE VIEW vw_faturamento_diario AS
SELECT 
  data,
  COUNT(id_venda) AS qtd_vendas,
  SUM(total) AS total_faturado
FROM vendas
GROUP BY data;