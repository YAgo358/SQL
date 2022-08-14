with base_clientes as 
(	-- base de cpfs aprovados na campanha de São João com chaves identitárias --
	select 
		cpf 
		,id_customer 
		,id_proposal_will
		,ds_origin 
	from tabela_clientes 
	where cpf in(select 						
					lpad(cast(c as varchar), 11, '0') as cpf
					from tabela_importada_s3 a)
)
, base_atrasos as 
( 	
	-- base para observar se o cliente está inad --
	select 
		id_customer
		,max(dt_snapshot_date) as dt_snapshot_date
		,dt_due_date as dt_vencimento
		,max(nr_days_paste_due) as nr_dias_em_atraso
	from tabela_atrasos
	group by 1,3
)
, base_fatura as 
(
	-- base com informações da fatura --
	select
		id_cliente as id_customer 
		,cpf
		,billing_period
		,dt_fechamento
		,dt_vencimento
		,dt_vencimento_util
		,vl_saldo_devedor
		,vl_novo_fatura as vl_fatura_mes
		,vl_pagamento_minimo
		,vl_parcelas_futuras
		,vl_pagamentos_efetivados
		,dt_pagamento
		,ds_status_pagamento
		,case when dt_pagamento is null then 'não pagou' 
			when (dt_pagamento is not null and ds_status_pagamento <> 'MINIMUM_NOT_ACHIEVED') then 'pagou'
			when ds_status_pagamento = 'MINIMUM_NOT_ACHIEVED' then 'fatura zerada'
			else 'null' end as ds_pagamento
		,case when dt_pagamento > dt_vencimento_util then true
			else false end flg_pagamento_atrasado 
		,case when dt_fechamento <= current_date then false
			else true end as flg_fatura_em_aberto
	from tabela_fatura
)
, base_antecipou as 
( 
	-- base para checar quanto antecipou --
	select 
		id_customer 
		, cpf
		, inv_dt_vencimento as dt_vencimento
		,sum(-1.0*(vl_total)) as vl_antecipado
	from tabela_movimentacoes
	where cd_tipo_mov = 2083
	group by 1,2,3
)
, base_total_debito_credito as 
(
	select 
		id_customer 
		, cpf
		, inv_dt_vencimento as dt_vencimento
		, id_fatura
		,max(inv_vl_total_debit) as total_debito
		,max(inv_vl_total_credit) as total_credito
		,max(inv_vl_total_debit) - max(inv_vl_total_credit) as diff_deb_cred
	from tabela_movimentacoes
	group by 1,2,3,4
)
select
	a.cpf 
	,a.id_customer
	,a.ds_origin as origem
	,c.dt_fechamento
	,b.dt_vencimento
	,c.dt_vencimento_util
	,c.dt_pagamento
	,c.flg_pagamento_atrasado
	,b.nr_dias_em_atraso
	,c.vl_fatura_mes
	,c.vl_parcelas_futuras
	,c.vl_pagamentos_efetivados
	,case when e.vl_antecipado is not null then true else false end as is_antecipou
	,case when e.vl_antecipado is null then 0.0 else e.vl_antecipado end as vl_antecipado
	,f.total_debito
	,f.total_credito
	,sum(f.diff_deb_cred) OVER(partition by a.id_customer order by b.dt_vencimento ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as saldo
	,sum(f.total_debito -(c.vl_pagamentos_efetivados + (case when e.vl_antecipado is null then 0.0 else e.vl_antecipado end))) OVER(partition by a.id_customer order by b.dt_vencimento ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as saldo_ajustado
from base_clientes a
left join base_atrasos b on (b.id_customer = a.id_customer)
left join base_fatura c on ( b.id_customer = c.id_customer and b.dt_vencimento = c.dt_vencimento)
left join base_antecipou e on (c.id_customer = e.id_customer and c.dt_vencimento = e.dt_vencimento)
left join base_total_debito_credito f on (c.id_customer = f.id_customer and c.dt_vencimento = f.dt_vencimento)
where dt_fechamento is not null
order by 1,5

