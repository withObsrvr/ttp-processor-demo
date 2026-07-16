package main

const horizonPaymentOperationPredicate = "(is_payment_op = true OR type IN (0, 1, 2, 8, 13))"
const horizonServingPaymentOperationPredicate = "(is_payment_op = true OR type_code IN (0, 1, 2, 8, 13))"
const horizonServingPaymentOperationPredicateQualified = "(o.is_payment_op = true OR o.type_code IN (0, 1, 2, 8, 13))"
const horizonServingSACPaymentEffectPredicate = "e.effect_type IN (2, 3, 96, 97)"
const horizonServingAccountPaymentEffectPredicate = "e.effect_type IN (0, 1, 2, 3, 96, 97)"
