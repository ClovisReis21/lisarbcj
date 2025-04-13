CREATE VIEW vw_vendas_por_vendedor AS
SELECT 
  v.id_vendedor,
  vd.nome,
  COUNT(v.id_venda) AS total_vendas,
  SUM(v.total) AS total_faturado
FROM vendas v
JOIN vendedores vd ON vd.id_vendedor = v.id_vendedor
GROUP BY v.id_vendedor, vd.nome;