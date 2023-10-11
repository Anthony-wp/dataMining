package com.datageneration.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Entity
@Table(schema = "testshop", name = "user")
@AllArgsConstructor
@NoArgsConstructor
public class User {
    @Id
    @Column(name = "id")
    private String id;
    @Column(name = "email")
    private String email;
    @Column(name = "first_name")
    private String firstName;
    @Column(name = "last_name")
    private String lastName;
    @Column(name = "postcode")
    private String postcode;
    @Column(name = "created")
    private Date created;
    @Column(name = "updated")
    private Date updated;
    @Column(name = "mobile_number")
    private String mobileNumber;
    @Column(name = "country")
    private String country;
}
