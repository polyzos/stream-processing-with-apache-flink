SELECT
    transactionId,
    UUID() AS cardNumber
FROM transactions
LIMIT 10;

SELECT
    transactionId,
    maskfn(UUID()) AS cardNumber
FROM transactions
LIMIT 10;

CREATE TEMPORARY VIEW sample AS
SELECT *
FROM transactions
LIMIT 10;


SELECT
    transactionId,
    operation,
    word,
    length
FROM sample, LATERAL TABLE(splitfn(operation));
