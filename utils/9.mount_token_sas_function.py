# Databricks notebook source
def mount_container(storage_account,container):
    mount_point = f"/mnt/{storage_account}/{container}"
    mounts = dbutils.fs.mounts()
    if any(mount.mountPoint == mount_point for mount in mounts):
        dbutils.fs.unmount(mount_point)

    dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
        mount_point = f"/mnt/{storage_account}/{container}",
        extra_configs = {f"fs.azure.sas.{container}.{storage_account}.blob.core.windows.net": "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-07-28T02:06:32Z&st=2023-07-25T18:06:32Z&spr=https&sig=JprIsoCjoUHU0ZuWKMh9wRuzZxPya0RiV05T2MVtZyM%3D"}
    )
    print(f"{mount_point} has been mounted.")
    display(dbutils.fs.mounts())
    

# COMMAND ----------

mount_container("sa70126482","raw")

# COMMAND ----------

mount_container("sa70126482","processed")

# COMMAND ----------

mount_container("sa70126482","presentation")

# COMMAND ----------

dbutils.fs
