with cte_dias as ( --Retorna os dias ate a data informada pelo usuario
  select distinct ts::date datavenda
    ,extract(day from ts) as diavenda
  from ( 
    select primeirodiadomes as tm from bsboard_filtros_globais where coalesce($DATA$,current_date) between primeirodiadomes and ultimodiadomes 
    union 
    select coalesce($DATA$,current_date)::date as tm 
  ) as t TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm) 
  order by 1 
) 

,cte_dia as ( --retorna o dia informado e a ultima hora do dia
  select coalesce($DATA$,current_date)::date  as c_day, 
  CASE WHEN coalesce($DATA$,current_date) = CURRENT_DATE THEN extract(hour from current_timestamp) 
  WHEN coalesce($DATA$,current_date) <> current_date THEN 23
  END as c_hour
)

,cte_gerencial_corrente as ( --Retorna ano e mes informado pelo usuario
  select anogerencial as anogerencial_corrente, mesgerencial as mesgerencial_corrente 
  from bsboard_filtros_globais, cte_dia cd 
  where coalesce(cd.c_day,current_date)::date between primeirodiadomes and ultimodiadomes 
)       

,cte_dy AS ( --Retorna as vendas do dia atual
  select v.anogerencial, v.mesgerencial 
    ,sum(v.valortotal) as valortotal
    ,sum(v.custototal) as custototal
    ,sum(v.impostototal) as impostototal
    ,sum(v.quantidade) as quantidade 
    ,count(distinct (case when v.quantidade >= 0 then v.vendachaveizicash else null end)) - count(distinct (case when v.quantidade < 0 then v.vendachaveizicash else null end)) as atendimentos
  from vendaaovivoitem_cubobase v
  where 1=1
    and $DATA$ = CURRENT_DATE
    and datavenda = current_date
    and ('0' in ($LOJAGRUPO$) or v.lojagrupocodigo in ($LOJAGRUPO$))
    and ('0' in ($LOJA$) or v.lojacodigo in ($LOJA$))
    and ('0' in ($SECOES$) or v.secaocodigo in ($SECOES$))
    and ('0' in (coalesce($SUPERVISOR$,'0')) or v.supervisorcodigo in (coalesce($SUPERVISOR$,'0')))
    and ('0' in ($PRODUTO$) or v.produtocodigo in ($PRODUTO$))
    and ('0' in ($GRUPOPRODUTO$) or v.grupoprodutocodigo in ($GRUPOPRODUTO$))
    and ('0' in ($SUBGRUPO$) or v.subgrupoprodutocodigo in ($SUBGRUPO$))
    and ('0' in ($COMPRADOR$) or v.compradorcodigo  in ($COMPRADOR$))
    and ('0' in ($FORNECEDOR$) or v.fornecedorcodigo in ($FORNECEDOR$))  
  group by 1,2
)

,cte_lms_dy AS ( --Retorna a venda do dia informado pelo usuario nos ultimos 12 meses
  select v.anogerencial, v.mesgerencial 
    ,sum(v.valortotal) as valortotal
    ,sum(v.custototal) as custototal
    ,sum(v.impostototal) as impostototal
    ,sum(v.quantidade) as quantidade 
    ,count(distinct (case when v.quantidade >= 0 then v.vendachaveizicash else null end)) - count(distinct (case when v.quantidade < 0 then v.vendachaveizicash else null end)) as atendimentos
  from vendaitem_cubobase v, cte_dia cd
   where 1=1
    and (
      (v.anogerencial=(select anogerencial_corrente from cte_gerencial_corrente) and v.mesgerencial<=(select mesgerencial_corrente from cte_gerencial_corrente)) 
      or (v.anogerencial=( (select anogerencial_corrente from cte_gerencial_corrente)-1) and v.mesgerencial>=(select mesgerencial_corrente from cte_gerencial_corrente))
    )
    and v.diavenda = extract(day from $DATA$::date) 
    and v.agrupadorhoravenda <= case when $DATA$ <> CURRENT_DATE then 23 else extract (hour from current_timestamp) + 1 end
  and ('0' in ($LOJAGRUPO$) or v.lojagrupocodigo in ($LOJAGRUPO$))
  and ('0' in ($LOJA$) or v.lojacodigo in ($LOJA$))
  and ('0' in ($SECOES$) or v.secaocodigo in ($SECOES$))
  and ('0' in (coalesce($SUPERVISOR$,'0')) or v.supervisorcodigo in (coalesce($SUPERVISOR$,'0')))
  and ('0' in ($PRODUTO$) or v.produtocodigo in ($PRODUTO$))
  and ('0' in ($GRUPOPRODUTO$) or v.grupoprodutocodigo in ($GRUPOPRODUTO$))
  and ('0' in ($SUBGRUPO$) or v.subgrupoprodutocodigo in ($SUBGRUPO$))
  and ('0' in ($COMPRADOR$) or v.compradorcodigo  in ($COMPRADOR$))
  and ('0' in ($FORNECEDOR$) or v.fornecedorcodigo in ($FORNECEDOR$))
  group by 1,2
)

,cte_lms_yd AS ( --Retorna a venda de todos os ultimos 12 meses com base no dia informado exceto o dia atual (Ex: se informar dia 25 ele retornar o mes 1 ate o dia 24, mes 2 ate o dia 24 etc)
    select v.anogerencial, v.mesgerencial
    ,sum(v.valortotal) as valortotal
    ,sum(v.custototal) as custototal
    ,sum(v.impostototal) as impostototal
    ,sum(v.quantidade) as quantidade 
    ,COALESCE(sum(v.atendimentos), count(distinct (case when v.quantidade >= 0 then v.vendachaveizicash else null end)) - count(distinct (case when v.quantidade < 0 then v.vendachaveizicash else null end))) as atendimentos

  from vendaitem_cubobase v, cte_dia cd
  where 1=1
    and ((v.anogerencial=(select anogerencial_corrente from cte_gerencial_corrente) and v.mesgerencial<=(select mesgerencial_corrente from cte_gerencial_corrente)) or (v.anogerencial=((select anogerencial_corrente from cte_gerencial_corrente)-1) and v.mesgerencial>=(select mesgerencial_corrente from cte_gerencial_corrente)))
    and v.diavenda <> extract(day from cd.c_day) 
  and (v.diavenda = 0 or (v.diavenda <> extract(day from cd.c_day) 
      and v.diavenda in (
        select extract(day from ts) as baseabase_dia 
        from ( 
          select primeirodiadomes as tm from bsboard_filtros_globais where  coalesce($DATA$,current_date) between primeirodiadomes and ultimodiadomes 
          union 
          select  coalesce($DATA$,current_date)::date as tm 
        ) as t TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)     
    )))       
and (
      ($DATA$ = CURRENT_DATE 
      and 'MTD' = vendachaveizicash 
      and (v.lojagrupocodigo = '-1') 
      and (v.lojacodigo = '-1')      
      and (v.secaocodigo = '-1')
      and '0' in ($LOJAGRUPO$)
    and '0' in ($LOJA$)
    and '0' in ($PRODUTO$)
    and '0' in ($GRUPOPRODUTO$)
    and '0' in (coalesce($SUPERVISOR$,'0'))
    and '0' in ($SUBGRUPO$)
    or ('0' not in ($SECOES$))
    and '0' in ($COMPRADOR$)
    and '0' in ($FORNECEDOR$))
    or  
       (
      (
      $DATA$ <> CURRENT_DATE
    or ('0' not in ($LOJAGRUPO$)) 
    or ('0' not in ($LOJA$)) 
    or ('0' not in ($PRODUTO$)) 
    or ('0' not in ($GRUPOPRODUTO$)) 
    or ('0' not in ($SUBGRUPO$)) 
    or ('0' not in (coalesce($SUPERVISOR$,'0')))
    or ('0' not in ($COMPRADOR$)) 
    or ('0' not in ($SECOES$))
    or ('0' not in ($FORNECEDOR$) ))
    
    and ('0' in ($LOJAGRUPO$) or v.lojagrupocodigo in ($LOJAGRUPO$))
    and ('0' in ($LOJA$) or v.lojacodigo in ($LOJA$))
    and ('0' in ($PRODUTO$) or v.produtocodigo in ($PRODUTO$))
         and ('0' in ($SECOES$) or v.secaocodigo in ($SECOES$))
    and ('0' in (coalesce($SUPERVISOR$,'0')) or v.supervisorcodigo in (coalesce($SUPERVISOR$,'0')))
    and ('0' in ($GRUPOPRODUTO$) or v.grupoprodutocodigo in ($GRUPOPRODUTO$))
    and ('0' in ($SUBGRUPO$) or v.subgrupoprodutocodigo in ($SUBGRUPO$))
    and ('0' in ($COMPRADOR$) or v.compradorcodigo  in ($COMPRADOR$))
    and ('0' in ($FORNECEDOR$) or v.fornecedorcodigo in ($FORNECEDOR$))
      and v.diavenda <> 0
      )
  )
  
  group by 1,2
)

--Agrupamento de todas as cte
  select v.anogerencial, v.mesgerencial 
    ,sum(v.quantidade) as quantidade 
    ,sum(v.valortotal) as valortotal 
    ,sum(v.atendimentos) as vendas 
    ,sum(v.valortotal) / nullif(sum(v.atendimentos), 0) as ticketmedio 
    ,(sum(v.quantidade::double precision) / nullif(sum(v.atendimentos), 0)) as itensmedio 
    ,(sum(v.valortotal) / nullif(sum(v.quantidade::double precision), 0)) as precomedio 
    ,sum(v.valortotal) / nullif(sum(v.custototal), 0) as markup 
    ,sum(v.valortotal) - sum(v.custototal) AS margemmonetaria
    ,((sum(v.valortotal) - sum(v.custototal))/ sum(v.valortotal-v.impostototal))*100.00 as margempercentual
  from (
    select * from cte_lms_dy
    union all
    select * from cte_lms_yd
    union all
    select * from cte_dy
    ) v 
  group by 1,2
  order by 1,2
