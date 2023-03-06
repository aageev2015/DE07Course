SELECT	clients.state, 
		COUNT(sales._id) TV_count
FROM silver.sales as sales
INNER JOIN gold.user_profiles_enriched AS clients
  ON sales.client_id = clients.client_id
WHERE '2022-09-01' <= sales.purchase_date AND sales.purchase_date <= '2022-09-10'
  AND sales.product IN ('TV')
  AND 20 <= clients.age AND clients.age <= 30
GROUP BY clients.state
ORDER BY TV_count desc
LIMIT 1
