package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class HolaDataset {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(HolaDataset.class.getSimpleName())
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> employeesDF = spark.read().json("src/main/resources/employees.json").cache();
        employeesDF.printSchema();
        employeesDF.show();

        System.out.println("Empleados con salario mayor a 3500:");
        employeesDF.filter((Row fila) -> {
            Long salario = fila.getLong(1);
            return salario > 3500;
        }).show();

        System.out.println("Empleados con salario mayor a 3500:");
        employeesDF.filter("salary > 3500").show();

        System.out.println("Empleados con salario mayor a 3500:");
        employeesDF.filter(col("salary").gt(3500)).show();

        Encoder<EmpleadoBean> encoderEmpleadoBean = Encoders.bean(EmpleadoBean.class);
        Dataset<EmpleadoBean> employeesDS = employeesDF.as(encoderEmpleadoBean);
        employeesDS.printSchema();
        employeesDS.show();

        System.out.println("Empleados con salario mayor a 3500:");
        employeesDS.filter((EmpleadoBean empleado) -> empleado.getSalary() > 3500).show();

        Dataset<Row> empleadosOriginalDF = employeesDS.toDF();
        JavaRDD<EmpleadoBean> empleadosRDD = employeesDS.javaRDD();

        System.out.println("Empleados:");
        employeesDF.withColumn("nombre", concat(col("name"), lit("EMPLEADO"))).show();

        System.out.println("Salarios:");
        employeesDF.select(col("salary").plus(1000)).show();

        System.out.println("Empleados:");
        employeesDF.select(col("salary").plus(1000), col("name")).show();

        jsc.close();
        spark.close();
    }

}
