-- a fact about production data that invented data cannot know
select * from {{ ref('account_totals') }} where total < 1000000000
