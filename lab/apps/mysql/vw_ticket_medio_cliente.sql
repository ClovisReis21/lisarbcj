CREATE VIEW vw_ticket_medio_cliente AS
SELECT 
  c.id_cliente,
  c.cliente,
  COUNT(v.id_venda) AS num_vendas,
  SUM(v.total) AS total_gasto,
  AVG(v.total) AS ticket_medio
FROM clientes c
JOIN vendas v ON v.id_cliente = c.id_cliente
GROUP BY c.id_cliente, c.cliente;