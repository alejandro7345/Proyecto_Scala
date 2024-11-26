// Databricks notebook source
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.expressions.Window




// COMMAND ----------

var dataF :String ="dbfs:/FileStore/co2.csv"


// COMMAND ----------

val df= spark.read 
.option("header","true")
.option("inferSchema","true")
.csv(dataF)



// COMMAND ----------

display{df}

// COMMAND ----------


val df_renamed = (
  df
 .withColumnRenamed( "Make" , "Marca" )
  .withColumnRenamed( "Model" , "Modelo" )
  .withColumnRenamed( "Vehicle Class" , "Clase_vehiculo" )
  .withColumnRenamed( "Engine Size(L)" , "Tamaño_motor(L)" )
  .withColumnRenamed( "Cylinders" , "Cantidad_Cilindros" )
  .withColumnRenamed( "Transmission" , "Tipo_Transmision" )
  .withColumnRenamed( "Fuel Type" , "Tipo_conbustible" )
  .withColumnRenamed( "Fuel Consumption City (L/100 km)" , "Consumo_cobus_ciudad(L/100 km)" )
  .withColumnRenamed( "Fuel Consumption Hwy (L/100 km)" , "Consumo_conbus_carretera(L/100 km)" )
  .withColumnRenamed( "Fuel Consumption Comb (L/100 km)" , "Consumo_conbus_comb(L/100 km)" )
  .withColumnRenamed( "Fuel Consumption Comb (mpg)" , "Consumo_combus_Comb(Millas*gal)" )
  .withColumnRenamed("CO2 Emissions(g/km)","emisiones_CO2(g/km)")
)


// COMMAND ----------

display
{
  df_renamed
}

// COMMAND ----------

println(s"Este DataSet tiene :")
println(s"${df_renamed.columns.length} Columnas ")
println(s"${df_renamed.count()} Reguistros")

// COMMAND ----------


  val dfCategori= df_renamed.withColumn(
  "Categoria_Combustible",
  when(col("Tipo_conbustible") ==="E", "E85 (E)")
    .when(col("Tipo_conbustible") === "Z", "Combustible Tradicional (Z)")
    .when(col("Tipo_conbustible") === "X", "Eco o Híbrido (x)")
    .when(col("Tipo_conbustible") === "D", "Diesel (D)")
    .otherwise("Gas_Natural")
)

val dfResult = dfCategorized.groupBy("Categoria_Combustible").count()

display{dfResult}





// COMMAND ----------

//display{df_renamed.filter($"Tamaño_motor(L)">2)}
//display{df_renamed.groupBy("Marca").avg("emisiones_CO2(g/km)").orderBy($"marca".asc)}


//-1¿ efecto del tipo de combustible en la produccion de CO2 y en la eficiencia de consumo?
val generalcom=df_renamed.withColumn( "Tipo_conbustible",
  when(col("Tipo_conbustible") === "E", "Ethanol-E85")
    .when(col("Tipo_conbustible") === "D", "Diesel")
    .when(col("Tipo_conbustible") === "Z", "Combustible tradicional")
    .when(col("Tipo_conbustible") === "N", "Gas natural")
    .when(col("Tipo_conbustible") === "X", "Híbrido")
    .otherwise("Desconocido") )


val emico2 = generalcom
.withColumn("Emiciones(g/L)" ,(col("emisiones_CO2(g/km)") / col("Consumo_conbus_comb(L/100 km)")) )
.withColumn("Eficiencia_consumo",(lit(100) /col("Consumo_conbus_comb(L/100 km)")))


.groupBy("Tipo_conbustible").agg(avg("Emiciones(g/L)") as ("Emiciones (g/L)"),avg("Eficiencia_consumo") as ("Eficiencia_Consumo"))






display{emico2}

// COMMAND ----------

//2_¿hay alguna relacion e entre el tamaño del motor y la cantidad de cilindros con respecto al consumo de combustible ?

val data=df_renamed.select("Cantidad_Cilindros","Tamaño_motor(L)",
"Consumo_cobus_ciudad(L/100 km)","Consumo_conbus_carretera(L/100 km)")



val motordata =data.groupBy("Tamaño_motor(L)")
.agg(
  avg("Consumo_cobus_ciudad(L/100 km)") as ("Promedio_C_Ciudad")
  ,avg("Consumo_conbus_carretera(L/100 km)") as ("Promedio_C_Carretera")
  )




display{motordata}

// COMMAND ----------

//2-

val cilidata=data.groupBy("Cantidad_Cilindros")
.agg(
  avg("Consumo_cobus_ciudad(L/100 km)") as ("Promedio_C_Ciudad")
  ,avg("Consumo_conbus_carretera(L/100 km)") as ("Promedio_C_Carretera"))

  display{cilidata}


// COMMAND ----------

//3-¿Cómo varía el consumo de combustible entre las diferentes clases de vehículos ?

val clasedata=df_renamed
.select("Clase_vehiculo","Consumo_combus_Comb(Millas*gal)")
 
.groupBy("Clase_vehiculo")
.agg(
 
  avg("Consumo_combus_Comb(Millas*gal)") as ("Promedio_Consumo_combinado")
  )
 


val dataventana = clasedata
  .orderBy($"Promedio_Consumo_combinado".desc)
  .withColumn("Valor_Previo",lag("Promedio_Consumo_combinado", 1) .over(windowSpec))
  .withColumn("diferencia_Con_siguiente",abs ( $"Valor_Previo"- $"Promedio_Consumo_combinado"  ))

  
  display{
    dataventana
  }
  





// COMMAND ----------

//4-¿Qué clases de vehículos, a pesar de tener motores más grandes, muestran una eficiencia notablemente mayor?"

val medata = df_renamed
.select("Clase_vehiculo","Tamaño_motor(L)","Consumo_combus_Comb(Millas*gal)")


val resumenPorClase = medata
  .groupBy("Clase_vehiculo")
  .agg(
    avg("Tamaño_motor(L)") as("Promedio_tamaño_motor"),
    avg("Consumo_combus_Comb(Millas*gal)") as("Promedio_Conbus_Comb")
  )


val clasesEficientes = resumenPorClase
  .where($"Promedio_Tamaño_Motor"> 3)
  .where($"Promedio_Conbus_Comb" > 25) 
  .orderBy($"Promedio_Conbus_Comb".desc)


display(clasesEficientes)


// COMMAND ----------

//5-¿q clase de vehiculo con q tipo de transmision tiene la mayor eficiencia de combustible?


val transmiciondata= df_renamed.select("Clase_vehiculo","Tipo_transmision",
"Consumo_combus_Comb(Millas*gal)")

val tradata= transmiciondata 
.groupBy("Clase_vehiculo","Tipo_transmision")
.agg(
  avg("Consumo_combus_Comb(Millas*gal)")  as ("Promedio_consumo_conbinado"), 
  count("Tipo_transmision") as ("Numero"))

val ordendata=tradata
.where($"Numero">=2).orderBy($"Promedio_consumo_conbinado".asc)



display {ordendata}



// COMMAND ----------

//6- ¿Qué porcentaje de emisiones totales proviene de cada tipo de combustible?

val emisiones=df_renamed
  .groupBy("Tipo_conbustible")
  .agg(sum("emisiones_CO2(g/km)")as("Total_Emisiones"))
  .withColumn("Porcentaje_Emisiones", col("Total_Emisiones") / sum("Total_Emisiones").over() * 100)

display(emisiones)


// COMMAND ----------

//7_ ¿ cuales son los autos con un consumo_combinado mayor al promedio y con emiciones de CO2 mayor a 250?


  val filtrodata = df_renamed
  .select("Modelo", "Consumo_conbus_comb(L/100 km)", "emisiones_CO2(g/km)")

val fil2 = filtrodata
  .withColumn("Promedio_Consumo", avg("Consumo_conbus_comb(L/100 km)").over())
  .where($"Consumo_conbus_comb(L/100 km)" > $"Promedio_Consumo")
  .orderBy($"emisiones_CO2(g/km)".desc)

val filtro3 = fil2
  .withColumn("Clasificacion_Emiciones",
    when(col("emisiones_CO2(g/km)") > 250, "Alta_contaminacion")
      .otherwise("Baja_contaminacion")
  )

 val filtro4 = filtro3.groupBy("Clasificacion_Emiciones").agg(count("Modelo"))


display{filtro3}

// COMMAND ----------

//8- ¿Cuáles son los 10  vehículos con mayor diferencia entre consumo en ciudad y carretera?
val consumodata=df_renamed
.select("Modelo","Marca","Consumo_cobus_ciudad(L/100 km)","Consumo_conbus_carretera(L/100 km)")
.withColumn("Diferencia",abs(col("Consumo_cobus_ciudad(L/100 km)")- col("Consumo_conbus_carretera(L/100 km)") ))
.limit(10)
.orderBy($"diferencia".desc)


display{consumodata}



// COMMAND ----------

//9-¿que porcentage de los vehiculos tienen transmicion automatica tradicional?

val totaltra=df_renamed.count()

val autodata=df_renamed.select("Modelo","Tipo_Transmision")

.groupBy("Tipo_Transmision").agg(count("Tipo_Transmision") as ("Cantidad"))

val porcenres= autodata
.withColumn("Porcentage",((col("cantidad")/totaltra)*lit(100)))

display{porcenres}


// COMMAND ----------

//10-¿Qué tipo de combustible es más eficiente para vehículos con motores grandes ?
val tamadata=df_renamed.select("Tipo_conbustible","Tamaño_motor(L)",
"Consumo_conbus_comb(L/100 km)")

.withColumn("Eficiencia(L/km)",(lit(100)/col("Consumo_conbus_comb(L/100 km)")))
.where($"Tamaño_motor(L)">3)
.groupBy("Tipo_conbustible").agg(avg("Eficiencia(L/km)") as ("Eficiencia_Promedio"))

.orderBy($"Eficiencia_Promedio".desc)
display{tamadata}

