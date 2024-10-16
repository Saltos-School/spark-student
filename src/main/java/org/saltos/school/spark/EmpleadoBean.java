package org.saltos.school.spark;

import java.io.Serializable;
import java.util.Objects;

public class EmpleadoBean implements Serializable {

    private String name;
    private Long salary;

    public Long getSalary() {
        return salary;
    }

    public void setSalary(Long salary) {
        this.salary = salary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmpleadoBean that = (EmpleadoBean) o;
        return Objects.equals(name, that.name) && Objects.equals(salary, that.salary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, salary);
    }

    @Override
    public String toString() {
        return "EmpleadoBean{" +
                "name='" + name + '\'' +
                ", salary=" + salary +
                '}';
    }

}
