package org.acme;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.component.dataset.DataSetSupport;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.UUID;

@ApplicationScoped
@Identifier("test-set")
public class MyDataSet extends DataSetSupport {

    @ConfigProperty(name = "myapp.test-set.size", defaultValue = "10000")
    long size;

    @Override
    protected Object createMessageBody(long messageIndex) {
        return UUID.randomUUID().toString();
    }

    @Override
    public long getSize() {
        return size;
    }
}
