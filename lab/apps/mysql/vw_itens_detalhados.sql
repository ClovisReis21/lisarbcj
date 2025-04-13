CREATE VIEW vw_itens_detalhados AS
SELECT 
  iv.id_venda,
  iv.id_produto,
  p.produto,
  v.id_vendedor,
  vd.nome AS nome_vendedor,
  iv.quantidade,
  iv.valor_unitario,
  iv.valor_total,
  iv.desconto
FROM itens_venda iv
JOIN produtos p ON p.id_produto = iv.id_produto
JOIN vendas v ON v.id_venda = iv.id_venda
JOIN vendedores vd ON vd.id_vendedor = v.id_vendedor;