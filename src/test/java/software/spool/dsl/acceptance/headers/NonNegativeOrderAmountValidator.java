package software.spool.dsl.acceptance.headers;

import software.spool.validator.api.Validate;
import software.spool.validator.api.Validator;

@Validate(value = Order.class, sourceId = "orders")
public class NonNegativeOrderAmountValidator implements Validator<Order> {
    @Override
    public boolean validate(Order order) {
        return order.amount() >= 0;
    }

    @Override
    public String failMessage(Order order) {
        return "amount must not be negative";
    }
}
