package main

const horizonPaymentOperationPredicate = "(is_payment_op = true OR type IN (0, 1, 2, 8, 13))"
const horizonServingPaymentOperationPredicate = "(is_payment_op = true OR type_code IN (0, 1, 2, 8, 13))"
