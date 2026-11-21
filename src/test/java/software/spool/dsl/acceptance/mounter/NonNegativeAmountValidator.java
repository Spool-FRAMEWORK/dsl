package software.spool.dsl.acceptance.mounter;

import software.spool.validator.api.Validate;
import software.spool.validator.api.Validator;

@Validate(value = Payment.class, sourceId = "payments")
public class NonNegativeAmountValidator implements Validator<Payment> {
    @Override
    public boolean validate(Payment payment) {
        return payment.amount() >= 0;
    }

    @Override
    public String failMessage(Payment payment) {
        return "amount must not be negative";
    }
}
