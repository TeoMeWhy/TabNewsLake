-- Databricks notebook source
with tb_summary as (

  select date(created_at) as dtReference,
         count(distinct id) as qtContent,
         count(distinct owner_id) as qtOwner
  from bronze.tabnews.contents
  group by 1
  order by 1

)

select *,
        sum(qtContent) over (partition by 1 order by dtReference) as acumContent,
        sum(qtOwner) over (partition by 1 order by dtReference) as acumOwner
from tb_summary

-- COMMAND ----------

select owner_username,
       count(distinct id) as qtdContent,
       sum(tabcoins) as qtTabcoins,
       sum(tabcoins) / count(distinct id) as qtTabCoinsContent

from bronze.tabnews.contents

group by 1

order by 3 desc

limit 10

-- COMMAND ----------

select *
from bronze.tabnews.contents
order by published_at desc

-- COMMAND ----------

with tb_top_users (

  select owner_username

  from bronze.tabnews.contents

  group by owner_username
  order by count(distinct id) desc

  limit 50

),

tb_summary_user as (

  select 
    date(created_at) as dtReference,
    owner_username,
    count(distinct id) as qtPublicacao

  from bronze.tabnews.contents
  group by 1,2

),

tb_summary_date as (

  select dtReference,
         sum(case when owner_username = 'FlaviaCarvalho' then qtPublicacao else 0 end) as FlaviaCarvalho,
         sum(case when owner_username = 'NewsletterOficial' then qtPublicacao else 0 end) as NewsletterOficial,
         sum(case when owner_username = 'gugadeschamps' then qtPublicacao else 0 end) as gugadeschamps,
         sum(case when owner_username = 'filipedeschamps' then qtPublicacao else 0 end) as filipedeschamps,
         sum(case when owner_username = 'viniciusbecaleti' then qtPublicacao else 0 end) as viniciusbecaleti,
         sum(case when owner_username = 'ghostnetrn' then qtPublicacao else 0 end) as ghostnetrn,
         sum(case when owner_username = 'joelcarneiro' then qtPublicacao else 0 end) as joelcarneiro,
         sum(case when owner_username = 'CarlosZiegler' then qtPublicacao else 0 end) as CarlosZiegler,
         sum(case when owner_username = 'Jetrom' then qtPublicacao else 0 end) as Jetrom,
         sum(case when owner_username = 'GabrielSozinho' then qtPublicacao else 0 end) as GabrielSozinho,
         sum(case when owner_username = 'NathanFirmo' then qtPublicacao else 0 end) as NathanFirmo,
         sum(case when owner_username = 'Diegiwg' then qtPublicacao else 0 end) as Diegiwg,
         sum(case when owner_username = 'rafael' then qtPublicacao else 0 end) as rafael,
         sum(case when owner_username = 'onlyDataFans' then qtPublicacao else 0 end) as onlyDataFans,
         sum(case when owner_username = 'tcarreira' then qtPublicacao else 0 end) as tcarreira,
         sum(case when owner_username = 'obrenoalvim' then qtPublicacao else 0 end) as obrenoalvim,
         sum(case when owner_username = 'bruiz' then qtPublicacao else 0 end) as bruiz,
         sum(case when owner_username = 'designliquido' then qtPublicacao else 0 end) as designliquido,
         sum(case when owner_username = 'andersoncampolina' then qtPublicacao else 0 end) as andersoncampolina,
         sum(case when owner_username = 'VictorManhani' then qtPublicacao else 0 end) as VictorManhani,
         sum(case when owner_username = 'matheuspazinati' then qtPublicacao else 0 end) as matheuspazinati,
         sum(case when owner_username = 'hebertcisco' then qtPublicacao else 0 end) as hebertcisco,
         sum(case when owner_username = 'allangrds' then qtPublicacao else 0 end) as allangrds,
         sum(case when owner_username = 'gabrielnunes' then qtPublicacao else 0 end) as gabrielnunes,
         sum(case when owner_username = 'vitormarkis' then qtPublicacao else 0 end) as vitormarkis,
         sum(case when owner_username = 'bokamoto' then qtPublicacao else 0 end) as bokamoto,
         sum(case when owner_username = 'viinilv' then qtPublicacao else 0 end) as viinilv,
         sum(case when owner_username = 'eikue' then qtPublicacao else 0 end) as eikue,
         sum(case when owner_username = 'rodrigoborges' then qtPublicacao else 0 end) as rodrigoborges,
         sum(case when owner_username = 'h4ck3rtr4d3r' then qtPublicacao else 0 end) as h4ck3rtr4d3r,
         sum(case when owner_username = 'GTEX' then qtPublicacao else 0 end) as GTEX,
         sum(case when owner_username = 'guiadeti' then qtPublicacao else 0 end) as guiadeti,
         sum(case when owner_username = 'LRamony' then qtPublicacao else 0 end) as LRamony,
         sum(case when owner_username = 'uriel' then qtPublicacao else 0 end) as uriel,
         sum(case when owner_username = 'planetsweb' then qtPublicacao else 0 end) as planetsweb,
         sum(case when owner_username = 'FernandoDaSilva' then qtPublicacao else 0 end) as FernandoDaSilva,
         sum(case when owner_username = 'theryston' then qtPublicacao else 0 end) as theryston,
         sum(case when owner_username = 'IsmaelMoreira' then qtPublicacao else 0 end) as IsmaelMoreira,
         sum(case when owner_username = 'Paulo42' then qtPublicacao else 0 end) as Paulo42,
         sum(case when owner_username = 'SrKim' then qtPublicacao else 0 end) as SrKim,
         sum(case when owner_username = 'Luisa' then qtPublicacao else 0 end) as Luisa,
         sum(case when owner_username = 'LucasEd' then qtPublicacao else 0 end) as LucasEd,
         sum(case when owner_username = 'Higordacosta' then qtPublicacao else 0 end) as Higordacosta,
         sum(case when owner_username = 'wendleypf' then qtPublicacao else 0 end) as wendleypf,
         sum(case when owner_username = 'cuzao' then qtPublicacao else 0 end) as cuzao,
         sum(case when owner_username = 'coffeeispower' then qtPublicacao else 0 end) as coffeeispower,
         sum(case when owner_username = 'johnathan' then qtPublicacao else 0 end) as johnathan,
         sum(case when owner_username = 'KitsuneSemCalda' then qtPublicacao else 0 end) as KitsuneSemCalda,
         sum(case when owner_username = 'Matrixs0beit' then qtPublicacao else 0 end) as Matrixs0beit,
         sum(case when owner_username = 'diogoneves07' then qtPublicacao else 0 end) as diogoneves07

  from tb_summary_user

  group by 1
  order by 1

), 

tb_final as (

    select dtReference,
          sum(FlaviaCarvalho) over (partition by 1 order by dtReference) as FlaviaCarvalho,
          sum(NewsletterOficial) over (partition by 1 order by dtReference) as NewsletterOficial,
          sum(gugadeschamps) over (partition by 1 order by dtReference) as gugadeschamps,
          sum(filipedeschamps) over (partition by 1 order by dtReference) as filipedeschamps,
          sum(viniciusbecaleti) over (partition by 1 order by dtReference) as viniciusbecaleti,
          sum(ghostnetrn) over (partition by 1 order by dtReference) as ghostnetrn,
          sum(joelcarneiro) over (partition by 1 order by dtReference) as joelcarneiro,
          sum(CarlosZiegler) over (partition by 1 order by dtReference) as CarlosZiegler,
          sum(Jetrom) over (partition by 1 order by dtReference) as Jetrom,
          sum(GabrielSozinho) over (partition by 1 order by dtReference) as GabrielSozinho,
          sum(NathanFirmo) over (partition by 1 order by dtReference) as NathanFirmo,
          sum(Diegiwg) over (partition by 1 order by dtReference) as Diegiwg,
          sum(rafael) over (partition by 1 order by dtReference) as rafael,
          sum(onlyDataFans) over (partition by 1 order by dtReference) as onlyDataFans,
          sum(tcarreira) over (partition by 1 order by dtReference) as tcarreira,
          sum(obrenoalvim) over (partition by 1 order by dtReference) as obrenoalvim,
          sum(bruiz) over (partition by 1 order by dtReference) as bruiz,
          sum(designliquido) over (partition by 1 order by dtReference) as designliquido,
          sum(andersoncampolina) over (partition by 1 order by dtReference) as andersoncampolina,
          sum(VictorManhani) over (partition by 1 order by dtReference) as VictorManhani,
          sum(matheuspazinati) over (partition by 1 order by dtReference) as matheuspazinati,
          sum(hebertcisco) over (partition by 1 order by dtReference) as hebertcisco,
          sum(allangrds) over (partition by 1 order by dtReference) as allangrds,
          sum(gabrielnunes) over (partition by 1 order by dtReference) as gabrielnunes,
          sum(vitormarkis) over (partition by 1 order by dtReference) as vitormarkis,
          sum(bokamoto) over (partition by 1 order by dtReference) as bokamoto,
          sum(viinilv) over (partition by 1 order by dtReference) as viinilv,
          sum(eikue) over (partition by 1 order by dtReference) as eikue,
          sum(rodrigoborges) over (partition by 1 order by dtReference) as rodrigoborges,
          sum(h4ck3rtr4d3r) over (partition by 1 order by dtReference) as h4ck3rtr4d3r,
          sum(GTEX) over (partition by 1 order by dtReference) as GTEX,
          sum(guiadeti) over (partition by 1 order by dtReference) as guiadeti,
          sum(LRamony) over (partition by 1 order by dtReference) as LRamony,
          sum(uriel) over (partition by 1 order by dtReference) as uriel,
          sum(planetsweb) over (partition by 1 order by dtReference) as planetsweb,
          sum(FernandoDaSilva) over (partition by 1 order by dtReference) as FernandoDaSilva,
          sum(theryston) over (partition by 1 order by dtReference) as theryston,
          sum(IsmaelMoreira) over (partition by 1 order by dtReference) as IsmaelMoreira,
          sum(Paulo42) over (partition by 1 order by dtReference) as Paulo42,
          sum(SrKim) over (partition by 1 order by dtReference) as SrKim,
          sum(Luisa) over (partition by 1 order by dtReference) as Luisa,
          sum(LucasEd) over (partition by 1 order by dtReference) as LucasEd,
          sum(Higordacosta) over (partition by 1 order by dtReference) as Higordacosta,
          sum(wendleypf) over (partition by 1 order by dtReference) as wendleypf,
          sum(cuzao) over (partition by 1 order by dtReference) as cuzao,
          sum(coffeeispower) over (partition by 1 order by dtReference) as coffeeispower,
          sum(johnathan) over (partition by 1 order by dtReference) as johnathan,
          sum(KitsuneSemCalda) over (partition by 1 order by dtReference) as KitsuneSemCalda,
          sum(Matrixs0beit) over (partition by 1 order by dtReference) as Matrixs0beit,
          sum(diogoneves07) over (partition by 1 order by dtReference) as diogoneves07

    from tb_summary_date

)

select * from tb_finaltb_top_users
