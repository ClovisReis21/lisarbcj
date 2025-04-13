CREATE VIEW vw_vendas_por_produto AS
SELECT 
  p.id_produto,
  p.produto,
  SUM(iv.quantidade) AS total_quantidade,
  SUM(iv.valor_unitario * iv.quantidade) AS total_bruto,
  SUM(iv.desconto) AS total_desconto,
  SUM(iv.valor_total) AS total_liquido
FROM itens_venda iv
JOIN produtos p ON p.id_produto = iv.id_produto
GROUP BY p.id_produto, p.produto;