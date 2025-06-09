# qualification_ols_star_model/transformations/dim_temps.py
import datetime
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType


def prepare_dim_temps(spark, date_ctrlm):
    try:
        if isinstance(date_ctrlm, str) and len(date_ctrlm) == 8:
            annee = date_ctrlm[:4]
            mois = date_ctrlm[4:6]
        else:
            current_date = datetime.datetime.now()
            annee = str(current_date.year)
            mois = f"{current_date.month:02d}"
        
        annee_mois = f"{annee}_{mois}"
        
        try:
            if hasattr(spark, 'hive_execute_query'):
                query = f"""
                SELECT 
                    '{annee_mois}' AS annee_mois,
                    '{annee}' AS annee,
                    '{mois}' AS mois
                """
                return spark.hive_execute_query(query)
            elif hasattr(spark, 'sql'):
                query = f"""
                SELECT 
                    '{annee_mois}' AS annee_mois,
                    '{annee}' AS annee,
                    '{mois}' AS mois
                """
                return spark.sql(query)
        except Exception:
            pass
        
        try:
            if hasattr(spark, 'read') and hasattr(spark.read, 'table'):
                tables = ["bv_personne_morale_bdt", "bv_tiers_bdt", "bv_departement", "bv_region"]
                for table in tables:
                    try:
                        df = spark.read.table(f"db_dev_ct3.{table}").limit(1)
                        return df.select(
                            lit(annee_mois).cast(StringType()).alias("annee_mois"),
                            lit(annee).cast(StringType()).alias("annee"),
                            lit(mois).cast(StringType()).alias("mois")
                        )
                    except Exception:
                        continue
        except Exception:
            pass
        
        try:
            query = f"""
            SELECT 
                '{annee_mois}' AS annee_mois,
                '{annee}' AS annee,
                '{mois}' AS mois
            """
            return spark.hive_execute_query(query)
        except Exception:
            try:
                if hasattr(spark, 'createDataFrame'):
                    df = spark.createDataFrame(
                        [(annee_mois, annee, mois)],
                        ["annee_mois", "annee", "mois"]
                    )
                    return df.select(
                        col("annee_mois").cast(StringType()),
                        col("annee").cast(StringType()),
                        col("mois").cast(StringType())
                    )
                else:
                    hard_query = f"""
                    SELECT 
                        '{annee_mois}' AS annee_mois,
                        '{annee}' AS annee,
                        '{mois}' AS mois
                    """
                    try:
                        return spark.sql(hard_query)
                    except:
                        try:
                            return spark.execute(hard_query)
                        except:
                            return spark.hive_execute_query(hard_query)
            except Exception:
                from pyspark.sql import SparkSession
                try:
                    spark_session = SparkSession.builder.getOrCreate()
                    df = spark_session.createDataFrame(
                        [(annee_mois, annee, mois)],
                        ["annee_mois", "annee", "mois"]
                    )
                    return df.select(
                        col("annee_mois").cast(StringType()),
                        col("annee").cast(StringType()),
                        col("mois").cast(StringType())
                    )
                except Exception:
                    return None
        
    except Exception:
        try:
            query = f"""
            SELECT 
                '{annee}_{mois}' AS annee_mois,
                '{annee}' AS annee,
                '{mois}' AS mois
            """
            return spark.hive_execute_query(query)
        except:
            return None