
      insert into "devdb"."dwh_sbx"."psa_clinet_bk_rk" ("client_rk", "clinet_bk")
    (
        select "client_rk", "clinet_bk"
        from "psa_clinet_bk_rk__dbt_tmp184004265796"
    )


  