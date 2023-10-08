package com.datageneration.service;

import com.datageneration.entity.Order;
import com.datageneration.entity.Product;
import com.datageneration.entity.User;
import com.datageneration.repository.OrderRepository;
import com.datageneration.repository.ProductRepository;
import com.datageneration.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataGenerationService {
    private final Random r = new Random();
    private final UserRepository customerRepository;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;

    @EventListener(ApplicationReadyEvent.class)
    public void generateData() {
        // customer params
        long customerCount = 1000;

        // product params
        long productCount = 200;
        int productCategories = 10;
        int maxProductStock = 50;
        BigDecimal minProductPrice = BigDecimal.ONE;
        BigDecimal maxProductPrice = BigDecimal.valueOf(100);

        // order params
        long orderCount = 20000; // parent orders (real order items rows will be greater)
        int maxOrderLines = 3; // max order rows
        int maxQuantity = 3; // max line qty
        Date upperOrderTimeBound = new Date(); // now
        Date lowerOrderTimeBound = new Date(System.currentTimeMillis() - 500 * 24 * 60 * 60 * 1000L); // 500 days ago

        // generation
        List<User> customers = customerRepository.saveAll(generateCustomers(customerCount));
        List<Product> products = productRepository.saveAll(generateProducts(productCount,
                minProductPrice, maxProductPrice, productCategories, maxProductStock));
        List<Order> orders = orderRepository.saveAll(generateOrders(orderCount, customers, products, maxOrderLines,
                maxQuantity, lowerOrderTimeBound, upperOrderTimeBound));

        log.info("Created {} customer, {} products, {} orders", customers.size(), products.size(), orders.size());
    }

    private List<User> generateCustomers(long amount) {
        long startValue = System.currentTimeMillis();

        return LongStream.range(startValue, startValue + amount)
                .mapToObj(id -> new User(
                        UUID.randomUUID().toString(),
                        "em" + id + "@gmail.com",
                        "Derek" + id,
                        "Brown" + id,
                        "E1 6AN",
                        new Date(),
                        new Date(),
                        StringUtils.reverse(String.valueOf(id)).substring(0, 10),
                        "UK"
                )).collect(Collectors.toList());
    }


    private List<Product> generateProducts(long amount, BigDecimal minPrice, BigDecimal maxPrice,
                                           int categories, int maxStock) {
        long startValue = System.currentTimeMillis();

        return LongStream.range(startValue, startValue + amount)
                .mapToObj(id -> {
                    BigDecimal exTax = randomBigDecimal(minPrice, maxPrice);
                    boolean available = r.nextBoolean();
                    String prodId = UUID.randomUUID().toString();

                    return new Product(
                            prodId,
                            "Product " + id,
                            "http://market/products/" + id,
                            "http://market/products/" + id + ".jpg",
                            available,
                            round(exTax),
                            "Category " + (r.nextInt(categories) + 1),
                            available ? r.nextInt(maxStock) + 1L : 0L,
                            new Date(),
                            new Date()
                    );
                }).collect(Collectors.toList());
    }

    private List<Order> generateOrders(long amount, List<User> customers, List<Product> products,
                                       int maxLines, int maxQty, Date minTimestamp, Date maxTimestamp) {
        long startValue = System.currentTimeMillis();

        return LongStream.range(startValue, startValue + amount)
                .mapToObj(id -> {
                    User customer = randomElement(customers);
                    String orderId = UUID.randomUUID().toString();

                    return randomElements(products, r.nextInt(maxLines) + 1L)
                            .stream()
                            .map(product -> {
                                Date timestamp = randomTimestamp(maxTimestamp, minTimestamp);

                                return new Order(
                                        UUID.randomUUID().toString(),
                                        orderId,
                                        customer.getId(),
                                        product.getId(),
                                        r.nextInt(maxQty) + 1L,
                                        timestamp,
                                        customer.getEmail(),
                                        customer.getMobileNumber(),
                                        "Stripe",
                                        "GBP",
                                        Math.random() > 0.05 ? "PAID" : "RETURNED",
                                        UUID.randomUUID().toString(),
                                        Math.random() > 0.2 ?
                                                product.getPrice().divide(BigDecimal.TEN, 2, BigDecimal.ROUND_HALF_UP) :
                                                BigDecimal.ZERO,
                                        product.getPrice(),
                                        product.getPrice(),
                                        timestamp,
                                        timestamp,
                                        "North Road",
                                        "82",
                                        "3th floor",
                                        "E28 3XL",
                                        "London",
                                        "London",
                                        "UK",
                                        "Main Street",
                                        "79",
                                        "1th floor",
                                        "NW21 3TP",
                                        "London",
                                        "London",
                                        "UK"
                                );
                            }).collect(Collectors.toList());
                }).flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private BigDecimal randomBigDecimal(BigDecimal minPrice, BigDecimal maxPrice) {
        return minPrice.add(maxPrice.subtract(minPrice).multiply(BigDecimal.valueOf(Math.random())));
    }

    private Date randomTimestamp(Date max, Date min) {
        return new Date(min.getTime() + (long) ((max.getTime() - min.getTime()) * Math.random()));
    }

    private <T> List<T> randomElements(List<T> list, long amount) {
        return LongStream.range(0, amount).mapToObj(l -> randomElement(list)).collect(Collectors.toList());
    }

    private <T> T randomElement(List<T> list) {
        if (CollectionUtils.isEmpty(list)) {
            throw new IllegalArgumentException("List can't be empty!");
        }

        return list.get(r.nextInt(list.size()));
    }

    private BigDecimal round(BigDecimal value) {
        return value.round(new MathContext(2, RoundingMode.HALF_UP));
    }

}
