package org.acme;

import org.apache.camel.Exchange;

import java.util.List;

public class MyCustomSplitter {
    public List splitMe(Exchange exchange) {
        List<Exchange> exchanges = exchange.getIn().getBody(List.class);
        List<String> bodies = exchanges.stream().map(ex -> ex.getIn().getBody(String.class)).toList();
        return bodies;
    }
}
