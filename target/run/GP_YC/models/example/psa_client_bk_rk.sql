
      insert into "devdb"."dwh_sbx"."psa_client_bk_rk" ("client_rk", "client_bk")
    (
        select "client_rk", "client_bk"
        from "psa_client_bk_rk__dbt_tmp152326504980"
    )


  