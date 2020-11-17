package com.learn.sbb.service;

import com.learn.sbb.model.Product;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@Service
public class ProductService {

    public ArrayList<Product> getProducts(){
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://localhost:8080/products";
        Product[] products = restTemplate.getForObject(url, Product[].class);
        ArrayList<Product> productArrayList = new ArrayList<Product>();
        for (Product p : products)
            productArrayList.add(p);

        return productArrayList;
    }


}
