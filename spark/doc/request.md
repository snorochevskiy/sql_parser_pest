
```
curl -X POST \
  -d '{"input":"file:///home/stas/dev/proj/poc_sql/spark/src/test/resources/test_inputs/test_input1.xml","output":"file:///home/stas/output/","format":"csv"}' \
  http://localhost:8097/submitInstruction
```

```
curl -X POST \
  -d '{"queryDescription":{"withBlocks":[{"dbName":"my_db","withViewName":"country","sql":"select * from countries"}],"finalSql":"select * from products join country on product.country_id = country.id"},"output":"file:///home/stas/output/","format":"csv"}' \
  http://localhost:8097/submitQueryDirectly
```