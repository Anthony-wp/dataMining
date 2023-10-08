package com.datageneration.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Data
@Entity
@Table(schema = "testshop", name = "order")
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    @Id
    @Column(name = "id")
    private String id;
    @Column(name = "order_id")
    private String orderId;
    @Column(name = "customer_id")
    private String customerId;
    @Column(name = "product_id")
    private String productId;
    @Column(name = "qty")
    private Long qty;
    @Column(name = "timestamp")
    private Date timestamp;
    @Column(name = "customer_email_address")
    private String customerEmailAddress;
    @Column(name = "customer_mobile_phone_no")
    private String customerMobilePhoneNo;
    @Column(name = "payment_method")
    private String paymentMethod;
    @Column(name = "currency")
    private String currency;
    @Column(name = "status")
    private String status;
    @Column(name = "discount_code")
    private String discountCode;
    @Column(name = "discount_amount")
    private BigDecimal discountAmount;
    @Column(name = "line_value_excluding_tax")
    private BigDecimal lineValueExcludingTax;
    @Column(name = "line_value_including_tax")
    private BigDecimal lineValueIncludingTax;

    @Column(name = "created")
    private Date created;
    @Column(name = "updated")
    private Date updated;

    @Column(name = "billing_address_line1")
    private String billingAddressLine1;
    @Column(name = "billing_address_line2")
    private String billingAddressLine2;
    @Column(name = "billing_address_line3")
    private String billingAddressLine3;
    @Column(name = "billing_address_line_postal_code")
    private String billingAddressLinePostalCode;
    @Column(name = "billing_address_line_region")
    private String billingAddressLineRegion;
    @Column(name = "billing_address_line_town")
    private String billingAddressLineTown;
    @Column(name = "billing_address_country")
    private String billingAddressCountry;

    @Column(name = "postal_address_line1")
    private String postalAddressLine1;
    @Column(name = "postal_address_line2")
    private String postalAddressLine2;
    @Column(name = "postal_address_line3")
    private String postalAddressLine3;
    @Column(name = "postal_address_postal_code")
    private String postalAddressPostalCode;
    @Column(name = "postal_address_region")
    private String postalAddressRegion;
    @Column(name = "postal_address_line_town")
    private String postalAddressLineTown;
    @Column(name = "postal_address_country")
    private String postalAddressCountry;
}
